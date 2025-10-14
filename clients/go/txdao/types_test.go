package txdao

import (
	"testing"

	"github.com/SundaeSwap-finance/ogmigo/ouroboros/chainsync/num"
	"github.com/SundaeSwap-finance/ogmigo/ouroboros/shared"
)

func TestUTxO_Value(t *testing.T) {
	tests := []struct {
		name    string
		utxo    UTxO
		wantAda int64
	}{
		{
			name: "ada only",
			utxo: UTxO{
				Address: "addr1...",
				Coin:    "1000000",
				Assets:  []Policy{},
			},
			wantAda: 1000000,
		},
		{
			name: "ada with assets",
			utxo: UTxO{
				Address: "addr1...",
				Coin:    "2000000",
				Assets: []Policy{
					{
						PolicyID: "policy1",
						Assets: []Asset{
							{
								Name:       "token1",
								OutputCoin: "100",
							},
						},
					},
				},
			},
			wantAda: 2000000,
		},
		{
			name: "multiple policies and assets",
			utxo: UTxO{
				Address: "addr1...",
				Coin:    "5000000",
				Assets: []Policy{
					{
						PolicyID: "policy1",
						Assets: []Asset{
							{Name: "token1", OutputCoin: "100"},
							{Name: "token2", OutputCoin: "200"},
						},
					},
					{
						PolicyID: "policy2",
						Assets: []Asset{
							{Name: "token3", OutputCoin: "300"},
						},
					},
				},
			},
			wantAda: 5000000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value := tt.utxo.Value()

			// Check ADA amount
			actualAda := value.AdaLovelace()
			if actualAda.Int64() != tt.wantAda {
				t.Errorf("Value() ADA = %v, want %v", actualAda.Int64(), tt.wantAda)
			}

			// Check asset count
			expectedAssetCount := 0
			for _, policy := range tt.utxo.Assets {
				expectedAssetCount += len(policy.Assets)
			}

			actualAssetCount := 0
			for _, assets := range value {
				for range assets {
					actualAssetCount++
				}
			}
			// Subtract 1 for the ADA entry
			actualAssetCount--

			if actualAssetCount != expectedAssetCount {
				t.Errorf("Value() asset count = %v, want %v", actualAssetCount, expectedAssetCount)
			}
		})
	}
}

func TestUTxO_Value_InvalidCoin(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for invalid coin amount")
		}
	}()

	utxo := UTxO{
		Address: "addr1...",
		Coin:    "invalid",
		Assets:  []Policy{},
	}

	_ = utxo.Value()
}

func TestUTxO_Value_InvalidAssetAmount(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic for invalid asset amount")
		}
	}()

	utxo := UTxO{
		Address: "addr1...",
		Coin:    "1000000",
		Assets: []Policy{
			{
				PolicyID: "policy1",
				Assets: []Asset{
					{
						Name:       "token1",
						OutputCoin: "invalid",
					},
				},
			},
		},
	}

	_ = utxo.Value()
}

func TestUTxO_Value_AssetContent(t *testing.T) {
	utxo := UTxO{
		Address: "addr1...",
		Coin:    "3000000",
		Assets: []Policy{
			{
				PolicyID: "abc123",
				Assets: []Asset{
					{Name: "token1", OutputCoin: "500"},
				},
			},
		},
	}

	value := utxo.Value()

	// Check that the asset exists in the value
	assetId := shared.FromSeparate("abc123", "token1")
	found := false
	var foundAmount num.Int

	for policyID, assets := range value {
		for assetName, amount := range assets {
			testAssetId := shared.FromSeparate(policyID, assetName)
			if testAssetId == assetId {
				found = true
				foundAmount = amount
				break
			}
		}
	}

	if !found {
		t.Errorf("Expected asset %v not found in value", assetId)
	}

	expectedAmount, ok := num.New("500")
	if !ok {
		t.Fatal("Failed to create expected amount")
	}

	if foundAmount.Int64() != expectedAmount.Int64() {
		t.Errorf("Asset amount = %v, want %v", foundAmount.Int64(), expectedAmount.Int64())
	}
}

func TestTx_Structure(t *testing.T) {
	// Test that Tx structure can be created and fields accessed
	tx := Tx{
		Pk:         "tx:abc123",
		Sk:         "tx",
		Block:      "block456",
		InChain:    true,
		Location:   "s3://bucket/key",
		Successful: true,
		Utxos: []UTxO{
			{
				Address: "addr1...",
				Coin:    "1000000",
			},
		},
		Collateral: UTxO{
			Address: "addr2...",
			Coin:    "5000000",
		},
	}

	if tx.Pk != "tx:abc123" {
		t.Errorf("Pk = %v, want tx:abc123", tx.Pk)
	}
	if tx.InChain != true {
		t.Errorf("InChain = %v, want true", tx.InChain)
	}
	if len(tx.Utxos) != 1 {
		t.Errorf("len(Utxos) = %v, want 1", len(tx.Utxos))
	}
}

func TestPolicy_Structure(t *testing.T) {
	policy := Policy{
		PolicyID: "policy123",
		Assets: []Asset{
			{Name: "token1", OutputCoin: "100"},
			{Name: "token2", OutputCoin: "200"},
		},
	}

	if policy.PolicyID != "policy123" {
		t.Errorf("PolicyID = %v, want policy123", policy.PolicyID)
	}
	if len(policy.Assets) != 2 {
		t.Errorf("len(Assets) = %v, want 2", len(policy.Assets))
	}
}

func TestAsset_Structure(t *testing.T) {
	asset := Asset{
		Name:       "mytoken",
		OutputCoin: "1000",
	}

	if asset.Name != "mytoken" {
		t.Errorf("Name = %v, want mytoken", asset.Name)
	}
	if asset.OutputCoin != "1000" {
		t.Errorf("OutputCoin = %v, want 1000", asset.OutputCoin)
	}
}

func TestDatum_Structure(t *testing.T) {
	datum := Datum{
		Hash:         "hash123",
		OriginalCbor: "cbor456",
	}

	if datum.Hash != "hash123" {
		t.Errorf("Hash = %v, want hash123", datum.Hash)
	}
	if datum.OriginalCbor != "cbor456" {
		t.Errorf("OriginalCbor = %v, want cbor456", datum.OriginalCbor)
	}
}
