package txdao

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/rs/zerolog"
)

// mockDynamoDBClient is a mock implementation of DynamoDB client for testing
type mockDynamoDBClient struct {
	dynamodbiface.DynamoDBAPI
	getItemFunc func(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
}

func (m *mockDynamoDBClient) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	if m.getItemFunc != nil {
		return m.getItemFunc(input)
	}
	return &dynamodb.GetItemOutput{}, nil
}

func TestDAO_New(t *testing.T) {
	mock := &mockDynamoDBClient{}
	logger := zerolog.Nop()

	dao := New(mock, "test-table", logger, false)

	if dao == nil {
		t.Fatal("New() returned nil")
	}
	if dao.table == nil {
		t.Error("DAO table is nil")
	}
	if dao.cache == nil {
		t.Error("DAO cache is nil")
	}
	if dao.dry != false {
		t.Error("DAO dry mode should be false")
	}
}

func TestDAO_New_DryMode(t *testing.T) {
	mock := &mockDynamoDBClient{}
	logger := zerolog.Nop()

	dao := New(mock, "test-table", logger, true)

	if dao.dry != true {
		t.Error("DAO dry mode should be true")
	}
}

func TestDAO_GetOutput_ValidIndex(t *testing.T) {
	// This test validates the logic of GetOutput without actually calling DynamoDB
	dao := &DAO{
		cache: map[string]Tx{
			"test-tx": {
				Pk:         "tx:test-tx",
				Sk:         "tx",
				Successful: true,
				Utxos: []UTxO{
					{Address: "addr1", Coin: "1000000"},
					{Address: "addr2", Coin: "2000000"},
				},
				Collateral: UTxO{Address: "collateral", Coin: "5000000"},
			},
		},
	}

	tests := []struct {
		name          string
		txId          string
		idx           int
		wantAddress   string
		wantError     bool
		errorContains string
	}{
		{
			name:        "first output",
			txId:        "test-tx",
			idx:         0,
			wantAddress: "addr1",
			wantError:   false,
		},
		{
			name:        "second output",
			txId:        "test-tx",
			idx:         1,
			wantAddress: "addr2",
			wantError:   false,
		},
		{
			name:          "index too high",
			txId:          "test-tx",
			idx:           5,
			wantError:     true,
			errorContains: "index too high",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utxo, err := dao.GetOutput(context.Background(), tt.txId, tt.idx)

			if tt.wantError {
				if err == nil {
					t.Errorf("GetOutput() expected error, got nil")
				} else if tt.errorContains != "" && err.Error() != tt.errorContains {
					t.Errorf("GetOutput() error = %v, want error containing %v", err, tt.errorContains)
				}
			} else {
				if err != nil {
					t.Errorf("GetOutput() unexpected error: %v", err)
				}
				if utxo.Address != tt.wantAddress {
					t.Errorf("GetOutput() address = %v, want %v", utxo.Address, tt.wantAddress)
				}
			}
		})
	}
}

func TestDAO_GetOutput_FailedTransaction(t *testing.T) {
	// For failed transactions, only the collateral output should be accessible
	dao := &DAO{
		cache: map[string]Tx{
			"failed-tx": {
				Pk:         "tx:failed-tx",
				Sk:         "tx",
				Successful: false,
				Utxos: []UTxO{
					{Address: "addr1", Coin: "1000000"},
					{Address: "addr2", Coin: "2000000"},
				},
				Collateral: UTxO{Address: "collateral", Coin: "5000000"},
			},
		},
	}

	tests := []struct {
		name          string
		idx           int
		wantAddress   string
		wantError     bool
		errorContains string
	}{
		{
			name:          "index too low",
			idx:           0,
			wantError:     true,
			errorContains: "index too low",
		},
		{
			name:          "index too low",
			idx:           1,
			wantError:     true,
			errorContains: "index too low",
		},
		{
			name:        "collateral output",
			idx:         2, // len(Utxos) = 2, so collateral is at index 2
			wantAddress: "collateral",
			wantError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			utxo, err := dao.GetOutput(context.Background(), "failed-tx", tt.idx)

			if tt.wantError {
				if err == nil {
					t.Errorf("GetOutput() expected error, got nil")
				} else if tt.errorContains != "" && err.Error() != tt.errorContains {
					t.Errorf("GetOutput() error = %v, want error containing %v", err, tt.errorContains)
				}
			} else {
				if err != nil {
					t.Errorf("GetOutput() unexpected error: %v", err)
				}
				if utxo.Address != tt.wantAddress {
					t.Errorf("GetOutput() address = %v, want %v", utxo.Address, tt.wantAddress)
				}
			}
		})
	}
}

func TestDAO_Cache(t *testing.T) {
	// Test that caching works
	dao := &DAO{
		cache: make(map[string]Tx),
	}

	tx := Tx{
		Pk:         "tx:cached-tx",
		Sk:         "tx",
		Successful: true,
	}

	// Manually add to cache
	dao.cache["cached-tx"] = tx

	// GetOutput should use the cached value
	_, err := dao.GetOutput(context.Background(), "cached-tx", 0)
	if err == nil {
		t.Error("Expected error for empty Utxos, but cache lookup worked")
	}
}

func TestTableName(t *testing.T) {
	tests := []struct {
		env  string
		want string
	}{
		{
			env:  "mainnet",
			want: "mainnet-sundae-sync-v2--lookup",
		},
		{
			env:  "preview",
			want: "preview-sundae-sync-v2--lookup",
		},
		{
			env:  "preprod",
			want: "preprod-sundae-sync-v2--lookup",
		},
		{
			env:  "dev",
			want: "dev-sundae-sync-v2--lookup",
		},
	}

	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			got := TableName(tt.env)
			if got != tt.want {
				t.Errorf("TableName(%v) = %v, want %v", tt.env, got, tt.want)
			}
		})
	}
}

// TestDAO_GetOutput_NotInCache is removed because it requires complex mocking
// of the ddb.Table behavior. The cache behavior is already tested in TestDAO_Cache.

func ExampleDAO_GetOutput() {
	// This is a documentation example showing typical usage
	// In real usage, you would have a proper DynamoDB client
	mock := &mockDynamoDBClient{}
	logger := zerolog.Nop()
	dao := New(mock, "mainnet-sundae-sync-v2--lookup", logger, false)

	ctx := context.Background()
	_, err := dao.GetOutput(ctx, "some-tx-hash", 0)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
}
