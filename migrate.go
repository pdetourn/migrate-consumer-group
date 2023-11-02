package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Action string

const ACTION_DO = Action("do")
const ACTION_TRY = Action("try")
const ACTION_FORCE = Action("do-force")

func usage() {
	fmt.Printf("Usage: %s <action> <broker> <topic> <existing.group.id> <new.group.id> [<login> <password>]\n", os.Args[0])
	fmt.Println("  action is one of:")
	fmt.Println("    try        do not proceed with any change")
	fmt.Println("    do         proceed but abort if new the group already exists")
	fmt.Println("    do-force   proceed even if new the group already exists")
	os.Exit(2)
}

func main() {
	ctx := context.Background()

	if len(os.Args) != 6 && len(os.Args) != 8 {
		usage()
	}
	action := Action(os.Args[1])
	broker := os.Args[2]
	topic := os.Args[3]
	existingGroup := os.Args[4]
	newGroup := os.Args[5]

	if action != ACTION_DO && action != ACTION_TRY && action != ACTION_FORCE {
		fmt.Printf("Unknown action: %s\n\n", action)
		usage()
	}

	transport := kafka.Transport{
		Dial: (&net.Dialer{
			Timeout: 20 * time.Second,
		}).DialContext,
		DialTimeout: 30 * time.Second,
	}

	if len(os.Args) == 8 {
		user := os.Args[6]
		password := os.Args[7]
		mechanism, scramError := scram.Mechanism(scram.SHA512, user, password)
		if scramError != nil {
			fmt.Printf("Error: %s\n", fmt.Errorf("failed to initialize SCRAM mechanism: %w", scramError))
			os.Exit(1)
		}
		transport.SASL = mechanism
	}

	client := kafka.Client{
		Addr:      kafka.TCP(broker),
		Transport: &transport,
	}

	fmt.Printf("Fetching offsets of topic %s for groups %s and %s...\n", topic, existingGroup, newGroup)
	describeResponse, describeError := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		GroupIDs: []string{existingGroup, newGroup},
	})
	if describeError != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to describe existing group: %w", describeError))
		os.Exit(1)
	}
	descriptionOfExisting := describeResponse.Groups[0]
	if descriptionOfExisting.Error != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to describe existing group: %w", descriptionOfExisting.Error))
		os.Exit(1)
	}

	fmt.Printf("Checking group %v state...\n", existingGroup)
	if descriptionOfExisting.GroupState != "Empty" {
		fmt.Printf("Error: existing group %s state is \"%s\" but should be \"Empty\"\n", existingGroup, descriptionOfExisting.GroupState)
		os.Exit(1)
	}

	fmt.Printf("Checking group %v state...\n", newGroup)
	descriptionOfNew := describeResponse.Groups[1]
	if descriptionOfNew.Error == nil && descriptionOfNew.GroupState != "Dead" {
		switch action {
		case ACTION_DO:
			fmt.Printf("Error: target group %s already exists for topic %s. Use action \"do-force\" to override\n", newGroup, topic)
			os.Exit(1)
		case ACTION_FORCE:
			fmt.Printf("[Warn] Target group %s already exists for topic %s and will be overriden\n", newGroup, topic)
			break
		case ACTION_TRY:
			fmt.Printf("[Warn] Target group %s already exists for topic %s. Use action \"%s\" to override\n", newGroup, topic, ACTION_FORCE)
		}
	}

	fmt.Printf("Retrieving topic %v metadata...\n", topic)
	topicMetadataResponse, topicMetadataError := client.Metadata(ctx, &kafka.MetadataRequest{
		Topics: []string{topic},
	})
	if topicMetadataError != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to retreive topic %s metadata: %w", topic, topicMetadataError))
		os.Exit(1)
	}
	topicMetadata := topicMetadataResponse.Topics[0]
	if topicMetadata.Error != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to retreive topic %s metadata: %w", topic, topicMetadata.Error))
		os.Exit(1)
	}

	fmt.Printf("Topic %s has %v partitions\n", topic, len(topicMetadata.Partitions))
	allPartitions := make([]int, len(topicMetadata.Partitions))
	for partitionId := 0; partitionId < len(allPartitions); partitionId++ {
		allPartitions[partitionId] = partitionId
	}

	fmt.Printf("Fetching existing offsets of topic %v for group %v...\n", topic, existingGroup)
	offsetFetchResponse, offsetFetchResponseError := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: existingGroup, Topics: map[string][]int{topic: allPartitions}})
	if offsetFetchResponseError != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to fetch offsets for topic %s: %w", topic, offsetFetchResponseError))
		os.Exit(1)
	}
	offsets := make([]int64, len(topicMetadata.Partitions))
	for partitionId := 0; partitionId < len(allPartitions); partitionId++ {
		offsetForPartitionResponse := offsetFetchResponse.Topics[topic][partitionId]
		if offsetForPartitionResponse.Error != nil {
			fmt.Printf("Error: %s\n", fmt.Errorf("failed to fetch offsets for topic %s:%v: %w", topic, partitionId, offsetForPartitionResponse.Error))
			os.Exit(1)
		}
		offsets[partitionId] = offsetForPartitionResponse.CommittedOffset
	}

	messagesToCommit := make([]kafka.Message, 0)
	for partitionId := 0; partitionId < len(offsets); partitionId++ {
		if offsets[partitionId] > 0 {
			fmt.Printf("Preparing to set offset of partition %s:%v to %v\n", topic, partitionId, offsets[partitionId])
			messagesToCommit = append(messagesToCommit, kafka.Message{
				Topic:     topic,
				Partition: partitionId,
				Offset:    offsets[partitionId] - 1,
			})
		} else {
			fmt.Printf("Skipping partition %s:%v as it has no commit offset\n", topic, partitionId)
		}
	}
	if len(messagesToCommit) > 0 {
		if action == ACTION_DO || action == ACTION_FORCE {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:     []string{broker},
				GroupID:     newGroup,
				GroupTopics: []string{topic},
			})
			commitError := reader.CommitMessages(ctx, messagesToCommit...)
			if commitError != nil {
				fmt.Printf("Error: %s\n", fmt.Errorf("failed to store offsets for topic %s: %w", topic, commitError))
				os.Exit(1)
			}
			fmt.Printf("Set %v offset(s) to topic %s for group %s\n", len(messagesToCommit), topic, newGroup)
			closeError := reader.Close()
			if closeError != nil {
				fmt.Printf("Error: %s\n", fmt.Errorf("failed to shutdown reader: %w", closeError))
				os.Exit(1)
			}
		} else {
			fmt.Printf("[Skipped - try mode] Set %v offsets to topic %s for group %s\n", len(messagesToCommit), topic, newGroup)
		}
	} else {
		fmt.Printf("No offsets set to group %s\n", newGroup)
	}

}
