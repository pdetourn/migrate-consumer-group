package main

import (
	"context"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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

	configMap := kafka.ConfigMap{
		"bootstrap.servers": broker,
	}

	// Handle extra parameters for SCRAM-based authentication
	if len(os.Args) == 8 {
		user := os.Args[6]
		password := os.Args[7]
		configMap["security.protocol"] = "SASL_SSL"
		configMap["sasl.mechanisms"] = "SCRAM-SHA-512"
		configMap["sasl.username"] = user
		configMap["sasl.password"] = password
	}

	admin, newAdminError := kafka.NewAdminClient(&configMap)
	if newAdminError != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to create admin client: %w", newAdminError))
		os.Exit(1)
	}

	// Describe both existing and new groups. It should succeed even if the new group does not exist.
	fmt.Printf("Fetching offsets of topic %s for groups %s and %s...\n", topic, existingGroup, newGroup)
	describeResponse, describeError := admin.DescribeConsumerGroups(ctx, []string{existingGroup, newGroup})
	if describeError != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to describe existing group: %w", describeError))
		os.Exit(1)
	}

	// Obviously we'd rather have the existing group... well... exist.
	descriptionOfExisting := describeResponse.ConsumerGroupDescriptions[0]
	if descriptionOfExisting.Error.Code() != kafka.ErrNoError {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to describe existing group: %w", descriptionOfExisting.Error))
		os.Exit(1)
	}

	// And also, we do not want to deal with an existing group that is a moving target, so we want the
	// group to be currently empty (i.e. all the consumers have been stopped).
	fmt.Printf("Checking group %v state...\n", existingGroup)
	if descriptionOfExisting.State != kafka.ConsumerGroupStateEmpty {
		fmt.Printf("Error: existing group %s state is \"%s\" but should be \"Empty\"\n", existingGroup, descriptionOfExisting.State)
		os.Exit(1)
	}

	// Let's check that the topic exist and get an idea on how many partitions there are.
	fmt.Printf("Retrieving topic %v metadata...\n", topic)
	topicMetadataResponse, topicMetadataError := admin.GetMetadata(&topic, false, 15_000)
	if topicMetadataError != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to retreive topic %s metadata: %w", topic, topicMetadataError))
		os.Exit(1)
	}
	topicMetadata := topicMetadataResponse.Topics[topic]
	if topicMetadata.Error.Code() != kafka.ErrNoError {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to retreive topic %s metadata: %w", topic, topicMetadata.Error))
		os.Exit(1)
	}

	fmt.Printf("Topic %s has %v partitions\n", topic, len(topicMetadata.Partitions))
	allPartitions := make([]int, len(topicMetadata.Partitions))
	for partitionId := 0; partitionId < len(allPartitions); partitionId++ {
		allPartitions[partitionId] = partitionId
	}

	// We're now going to do some efforts to check if the new group already exists
	fmt.Printf("Checking group %v state...\n", newGroup)
	descriptionOfNew := describeResponse.ConsumerGroupDescriptions[1]
	if descriptionOfNew.Error.Code() == kafka.ErrNoError && descriptionOfNew.State != kafka.ConsumerGroupStateDead {
		// It seems to exist, so let's try to see if any of the offsets are set
		fmt.Printf("Fetching existing offsets of topic %v for group %v...\n", topic, newGroup)
		newOffsetFetchResponse, newOffsetFetchResponseError := admin.ListConsumerGroupOffsets(
			ctx, []kafka.ConsumerGroupTopicPartitions{
				{
					Group: newGroup,
				},
			},
		)
		if newOffsetFetchResponseError != nil {
			fmt.Printf("Error: %s\n", fmt.Errorf("failed to fetch offsets of topic %s for group %s: %w", topic, newGroup, newOffsetFetchResponseError))
			os.Exit(1)
		}
		atLeastOneOffset := false
		for i := 0; i < len(newOffsetFetchResponse.ConsumerGroupsTopicPartitions[0].Partitions); i++ {
			offsetForPartitionResponse := newOffsetFetchResponse.ConsumerGroupsTopicPartitions[0].Partitions[i]
			if offsetForPartitionResponse.Error == nil && offsetForPartitionResponse.Offset > 0 && *offsetForPartitionResponse.Topic == topic {
				fmt.Printf("Existing offset %s:%v is %v\n", topic, offsetForPartitionResponse.Partition, offsetForPartitionResponse.Offset)
				atLeastOneOffset = true
			}
		}

		// So not only the new group already exists, but it has some existing offsets. That's not good, as we're going to
		// override these and lose them forever...
		if atLeastOneOffset {
			switch action {
			case ACTION_DO:
				fmt.Printf("Error: target group %s already exists for topic %s. Use action \"do-force\" to override\n", newGroup, topic)
				os.Exit(1)
			case ACTION_FORCE:
				fmt.Printf("[Warn] Target group %s already exists for topic %s and will be overriden\n*** WRITE THE ABOVE EXISTING OFFSETS DOWN! ***\n*** YOU MAY NEED THEM TO RECOVER IF ANYTHING GOES WRONG! ***\n", newGroup, topic)
				break
			case ACTION_TRY:
				fmt.Printf("[Warn] Target group %s already exists for topic %s. Use action \"%s\" to override\n", newGroup, topic, ACTION_FORCE)
			}
		}

	}

	// Let's read the offsets that we need to copy.
	fmt.Printf("Fetching existing offsets of topic %v for group %v...\n", topic, existingGroup)
	offsetFetchResponse, offsetFetchResponseError := admin.ListConsumerGroupOffsets(ctx, []kafka.ConsumerGroupTopicPartitions{
		{
			Group: existingGroup,
		},
	})
	if offsetFetchResponseError != nil {
		fmt.Printf("Error: %s\n", fmt.Errorf("failed to fetch offsets of topic %s for group %s: %w", topic, newGroup, offsetFetchResponseError))
		os.Exit(1)
	}
	offsets := map[int32]kafka.Offset{}
	for i := 0; i < len(offsetFetchResponse.ConsumerGroupsTopicPartitions[0].Partitions); i++ {
		offsetForPartitionResponse := offsetFetchResponse.ConsumerGroupsTopicPartitions[0].Partitions[i]
		if *offsetForPartitionResponse.Topic == topic {
			if offsetForPartitionResponse.Error != nil {
				fmt.Printf("Error: %s\n", fmt.Errorf("failed to fetch offsets of topic %s:%v for group %v: %w", topic, offsetForPartitionResponse.Partition, newGroup, offsetForPartitionResponse.Error))
				os.Exit(1)
			}
			offsets[offsetForPartitionResponse.Partition] = offsetForPartitionResponse.Offset
		}
	}

	// And let's prepare the commit payload.
	partitionsToCommit := make([]kafka.TopicPartition, 0)
	for partitionId, offset := range offsets {
		if offset > 0 {
			fmt.Printf("Preparing to set offset of partition %s:%v to %v\n", topic, partitionId, offsets[partitionId])
			partitionsToCommit = append(partitionsToCommit, kafka.TopicPartition{
				Topic:     &topic,
				Partition: partitionId,
				Offset:    offset,
			})
		} else {
			fmt.Printf("Skipping partition %s:%v as it has no commit offset\n", topic, partitionId)
		}
	}

	// Here we go, let's do the actual update.
	if len(partitionsToCommit) > 0 {
		if action == ACTION_DO || action == ACTION_FORCE {
			// Update the offsets.
			_, alterError := admin.AlterConsumerGroupOffsets(ctx, []kafka.ConsumerGroupTopicPartitions{
				{
					Group:      newGroup,
					Partitions: partitionsToCommit,
				},
			})
			if alterError != nil {
				fmt.Printf("Error: %s\n", fmt.Errorf("failed to alter offsets of topic %s for group %v: %w", topic, newGroup, alterError))
				os.Exit(1)
			}
			fmt.Printf("Set %v offset(s) to topic %s for group %s\n", len(partitionsToCommit), topic, newGroup)
		} else {
			fmt.Printf("[Skipped - try mode] Set %v offsets to topic %s for group %s\n", len(partitionsToCommit), topic, newGroup)
		}
	} else {
		fmt.Printf("No offsets set to group %s\n", newGroup)
	}

}
