package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2Type "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"log"
)

func main() {
	// Load the Shared AWS Configuration (~/.aws/config)
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.TODO()
	client := ec2.NewFromConfig(cfg)
	res, _ := client.DescribeVpcs(ctx, nil)
	fmt.Println(res)
}

func GetInstances(api ec2.Client) ([]ec2Type.Instance, error) {
	var result []ec2Type.Instance
	out, err := api.DescribeInstances(context.TODO(), nil)
	if err != nil {
		return result, err
	}
	for _, r := range out.Reservations {
		result = append(result, r.Instances...)
	}
	return result, nil
}

func GetVpcs(api ec2.Client) ([]ec2Type.Vpc, error) {
	var result []ec2Type.Vpc
	out, err := api.DescribeVpcs(context.TODO(), nil)
	if err != nil {
		return result, err
	}
	result = append(result, out.Vpcs...)
	return result, nil
}

func GetSubNets(api ec2.Client) ([]ec2Type.Subnet, error) {
	var result []ec2Type.Subnet
	out, err := api.DescribeSubnets(context.TODO(), nil)
	if err != nil {
		return result, err
	}
	result = append(result, out.Subnets...)
	return result, nil
}
