package dynamoDB

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"time"
)

type DDBConn struct {
	TableName string
	CTX       context.Context
	client    *dynamodb.DynamoDB
}

func (dd *DDBConn) CreateTable() error {
	// Check table exists
	inputD := dynamodb.DescribeTableInput{TableName: &dd.TableName}
	_, err := dd.client.DescribeTable(&inputD)
	if err == nil {
		return nil
	}
	if awsErr, ok := err.(awserr.Error); ok {
		if awsErr.Code() != dynamodb.ErrCodeResourceNotFoundException {
			return err
		}
	} else {
		return err
	}
	// Create table if not exists
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("name"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("name"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(dd.TableName),
	}
	_, err = dd.client.CreateTable(input)
	return err
}

func (dd *DDBConn) Select(w map[string]interface{}) (*[]WsRow, error) {
	var result []WsRow
	if len(w) < 1 {
		// Full Table Scan
		input := &dynamodb.ScanInput{
			AttributesToGet:           nil,
			ConditionalOperator:       nil,
			ConsistentRead:            nil,
			ExclusiveStartKey:         nil,
			ExpressionAttributeNames:  nil,
			ExpressionAttributeValues: nil,
			FilterExpression:          nil,
			IndexName:                 nil,
			Limit:                     nil,
			ProjectionExpression:      nil,
			ReturnConsumedCapacity:    nil,
			ScanFilter:                nil,
			Segment:                   nil,
			Select:                    nil,
			TableName:                 &dd.TableName,
			TotalSegments:             nil,
		}
		res, err := dd.client.Scan(input)
		if err != nil {
			return nil, err
		}
		for _, r := range res.Items {
			wsRow := WsRow{}
			err = dynamodbattribute.UnmarshalMap(r, &wsRow)
			if err != nil {
				continue
			}
			result = append(result, wsRow)
		}
		return &result, nil
	} else {

	}

	return &result, nil
}

func (dd *DDBConn) Insert(r WsRow) error {
	av, err := dynamodbattribute.MarshalMap(r)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(dd.TableName),
	}
	_, err = dd.client.PutItem(input)
	if err != nil {
		return err
	}
	return nil
}

func (dd *DDBConn) Set(name string, state string) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":r": {
				S: aws.String(state),
			},
			":u": {
				S: aws.String(time.Now().String()),
			},
		},
		ExpressionAttributeNames: map[string]*string{
			"#S": aws.String("state"),
		},
		TableName: aws.String(dd.TableName),
		Key: map[string]*dynamodb.AttributeValue{
			"name": {
				S: aws.String(name),
			},
		},
		ReturnValues:     aws.String("UPDATED_NEW"),
		UpdateExpression: aws.String("set #S = :r, update_date = :u"),
	}
	_, err := dd.client.UpdateItem(input)
	return err
}
