// Package txdao provides a Go client for accessing transaction data indexed by sundae-sync-v2.
//
// This package defines the data structures and DAO (Data Access Object) for reading
// transaction information from DynamoDB that was written by the sundae-sync-v2 Rust indexer.
package txdao

import (
	"github.com/SundaeSwap-finance/ogmigo/ouroboros/chainsync/num"
	"github.com/SundaeSwap-finance/ogmigo/ouroboros/shared"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Datum struct {
	Hash         string `dynamodbav:"hash"`         // base64 encoded
	OriginalCbor string `dynamodbav:"originalCbor"` // base64 encoded
	Payload      dynamodb.AttributeValue
}

type Script struct {
	PlutusV1 string `dynamodbav:"plutusV1,omitempty"` // base64 encoded
	PlutusV2 string `dynamodbav:"plutusV2,omitempty"` // base64 encoded
	PlutusV3 string `dynamodbav:"plutusV3,omitempty"` // base64 encoded
}

type Asset struct {
	Name       string // base64 encoded token name
	OutputCoin string // primary key "output_coin", legacy key "outputCoin"
}

// UnmarshalDynamoDBAttributeValue accepts both the current Rust writer format
// (serde default, snake_case: "output_coin") and the legacy camelCase format
// ("outputCoin") that still exists in older records on long-lived environments
// like mainnet.
func (a *Asset) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if item == nil || item.M == nil {
		return nil
	}
	if n, ok := item.M["name"]; ok && n.S != nil {
		a.Name = *n.S
	}
	key := "output_coin"
	if _, ok := item.M[key]; !ok {
		key = "outputCoin"
	}
	if v, ok := item.M[key]; ok && v.S != nil {
		a.OutputCoin = *v.S
	}
	return nil
}

type Policy struct {
	PolicyID string // base64 encoded; primary key "policy_id", legacy "policyId"
	Assets   []Asset
}

// UnmarshalDynamoDBAttributeValue accepts both "policy_id" (current) and
// "policyId" (legacy). See Asset.UnmarshalDynamoDBAttributeValue.
func (p *Policy) UnmarshalDynamoDBAttributeValue(item *dynamodb.AttributeValue) error {
	if item == nil || item.M == nil {
		return nil
	}
	key := "policy_id"
	if _, ok := item.M[key]; !ok {
		key = "policyId"
	}
	if v, ok := item.M[key]; ok && v.S != nil {
		p.PolicyID = *v.S
	}
	if assets, ok := item.M["assets"]; ok && assets.L != nil {
		p.Assets = make([]Asset, 0, len(assets.L))
		for _, el := range assets.L {
			var a Asset
			if err := a.UnmarshalDynamoDBAttributeValue(el); err != nil {
				return err
			}
			p.Assets = append(p.Assets, a)
		}
	}
	return nil
}

type UTxO struct {
	Address  string   `dynamodbav:"address"`            // base64 encoded
	Coin     string   `dynamodbav:"coin"`               // lovelace
	Assets   []Policy `dynamodbav:"assets"`
	DatumCBOR  []byte `dynamodbav:"datum,omitempty"`  // raw datum CBOR
	Script *Script `dynamodbav:"script,omitempty"` // reference script
}

func (u UTxO) Value() shared.Value {
	ada, ok := num.New(u.Coin)
	if !ok {
		panic("invalid utxo")
	}
	value := shared.CreateAdaValue(ada.Int64())
	for _, policy := range u.Assets {
		for _, asset := range policy.Assets {
			assetId := shared.FromSeparate(policy.PolicyID, asset.Name)
			qty, ok := num.New(asset.OutputCoin)
			if !ok {
				panic("invalid utxo")
			}
			value.AddAsset(shared.Coin{AssetId: assetId, Amount: qty})
		}
	}
	return value
}

type Tx struct {
	Pk         string `dynamodbav:"pk" ddb:"hash"`
	Sk         string `dynamodbav:"sk" ddb:"range"`
	Block      string `dynamodbav:"block"`
	InChain    bool   `dynamodbav:"in_chain"`
	Location   string `dynamodbav:"location"`
	Successful bool   `dynamodbav:"successful"`
	Utxos      []UTxO `dynamodbav:"utxos"`
	Collateral UTxO   `dynamodbav:"collateral_out"`
}
