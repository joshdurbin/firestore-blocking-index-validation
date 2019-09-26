package main

import (
	"context"
	"fmt"
	"time"

	apiv1 "cloud.google.com/go/firestore/apiv1/admin"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/sirupsen/logrus"
	adminpb "google.golang.org/genproto/googleapis/firestore/admin/v1"
)

const (
	timeInBetweenIndexCreationStatusChecks = time.Second * 10
)

func main() {

	endPoint := ""
	credentialsFile := "/path/to/creds"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var args []option.ClientOption

	if len(endPoint) != 0 {
		args = append(args, option.WithoutAuthentication(), option.WithEndpoint(endPoint), option.WithGRPCDialOption(grpc.WithInsecure()))
	} else if len(credentialsFile) != 0 {
		args = append(args, option.WithCredentialsFile(credentialsFile))
	}

	firestoreAdminClient, _ := apiv1.NewFirestoreAdminClient(ctx, args...)

	defer firestoreAdminClient.Close()
	tuples := make([]*indexTuple, 0)

	// names and locations
	tuples = append(tuples, &indexTuple{
		FirstField:  "name",
		SecondField: "location",
	})

	// names and employers
	tuples = append(tuples, &indexTuple{
		FirstField:  "name",
		SecondField: "employer",
	})

	indexParent := fmt.Sprintf("projects/%s/databases/(default)/collectionGroups/%s", "project-id", "collection-id")

	ensureIndexes(ctx, firestoreAdminClient, tuples, indexParent)
}

type indexTuple struct {
	FirstField  string
	SecondField string
}

func ensureIndexes(ctx context.Context, adminSvc *apiv1.FirestoreAdminClient, tuples []*indexTuple, indexParent string) error {
	ascendingFieldOrder := adminpb.Index_IndexField_Order_{
		Order: adminpb.Index_IndexField_ASCENDING,
	}

	tuplesToIndexNames := make(map[*indexTuple]string)
	// create the indexes
	for _, tuple := range tuples {
		fields := make([]*adminpb.Index_IndexField, 0)
		fields = append(fields, &adminpb.Index_IndexField{
			FieldPath: tuple.FirstField,
			ValueMode: &ascendingFieldOrder,
		})
		fields = append(fields, &adminpb.Index_IndexField{
			FieldPath: tuple.SecondField,
			ValueMode: &ascendingFieldOrder,
		})
		operation, err := adminSvc.CreateIndex(ctx, &adminpb.CreateIndexRequest{
			Parent: indexParent,
			Index: &adminpb.Index{
				QueryScope: adminpb.Index_COLLECTION,
				Fields:     fields,
			},
		})
		if err != nil && status.Convert(err).Code() != codes.AlreadyExists {
			log.Debug("non-already exists error, returning.")
			return status.Convert(err).Err()
		}
		if operation != nil {
			meta := adminpb.IndexOperationMetadata{}
			_ = meta.XXX_Unmarshal(operation.Metadata.Value)
			tuplesToIndexNames[tuple] = meta.Index
		}
	}

	// check for statuses and block
	for {
		if len(tuplesToIndexNames) == 0 {
			break
		}
		time.Sleep(timeInBetweenIndexCreationStatusChecks)
		for tuple, name := range tuplesToIndexNames {
			index, _ := adminSvc.GetIndex(ctx, &adminpb.GetIndexRequest{Name: name})
			log.Infof("Index for tuple %s-%s, %s, state is %s.", tuple.FirstField, tuple.SecondField, index.Name, index.State.String())
			if index.State == adminpb.Index_READY {
				delete(tuplesToIndexNames, tuple)
			}
		}
	}

	return nil
}
