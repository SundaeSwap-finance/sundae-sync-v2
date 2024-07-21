use pallas::ledger::addresses::Address;
use serde::{Deserialize, Serialize};
use utxorpc::spec::cardano::{Block, Multiasset, Tx, TxOutput};

#[derive(Serialize, Deserialize, Debug)]
pub enum TokenFilter {
    Policy {
        #[serde(with = "serde_bytes")]
        policy: Vec<u8>,
    },
    AssetId {
        #[serde(with = "serde_bytes")]
        policy: Vec<u8>,
        #[serde(with = "serde_bytes")]
        asset_name: Vec<u8>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub enum FilterConfig {
    All(Vec<Box<FilterConfig>>),
    Any(Vec<Box<FilterConfig>>),
    Address {
        #[serde(with = "serde_bytes")]
        address: Vec<u8>,
    },
    Payment {
        #[serde(with = "serde_bytes")]
        payment: Vec<u8>,
    },
    Stake {
        #[serde(with = "serde_bytes")]
        stake: Vec<u8>,
    },
    Spent(TokenFilter),
    Mint(TokenFilter),
    Withdraw {
        #[serde(with = "serde_bytes")]
        credential: Vec<u8>,
    },
    Signed {
        #[serde(with = "serde_bytes")]
        credential: Vec<u8>,
    },
}

fn any_inputs_and_outputs<F>(tx: &Tx, f: F) -> bool
where
    F: Fn(&TxOutput) -> bool,
{
    let (inputs, outputs) = (tx.inputs.iter(), tx.outputs.iter());
    if outputs.into_iter().any(|output| f(output)) {
        return true;
    }
    if inputs
        .into_iter()
        // TODO: find out why as_output is coming back none so often
        .any(|input| input.as_output.as_ref().map_or(false, |output| f(output)))
    {
        return true;
    }
    return false;
}

impl TokenFilter {
    pub fn applies(&self, assets: &Vec<Multiasset>) -> bool {
        match self {
            TokenFilter::Policy { policy } => assets.iter().any(|a| a.policy_id == policy),
            TokenFilter::AssetId { policy, asset_name } => assets
                .iter()
                .any(|a| a.policy_id == policy && a.assets.iter().any(|a| a.name == asset_name)),
        }
    }
}

impl FilterConfig {
    pub fn applies_block(&self, b: &Block) -> bool {
        b.body
            .as_ref()
            .map_or(true, |body| body.tx.iter().any(|tx| self.applies(tx)))
    }

    pub fn applies(&self, tx: &Tx) -> bool {
        match self {
            FilterConfig::All(criteria) => criteria.iter().all(|c| c.applies(tx)),
            FilterConfig::Any(criteria) => criteria.iter().any(|c| c.applies(tx)),
            FilterConfig::Address { address } => {
                any_inputs_and_outputs(tx, |out| address.eq(&out.address.to_vec()))
            }
            FilterConfig::Payment { payment } => any_inputs_and_outputs(tx, |out| {
                let out_addr = Address::from_bytes(&out.address);
                match out_addr {
                    Ok(Address::Byron(b)) => b.to_vec().eq(payment),
                    Ok(Address::Shelley(s)) => s.payment().to_vec().eq(payment),
                    Ok(Address::Stake(_)) => false,
                    _ => true,
                }
            }),
            FilterConfig::Stake { stake } => any_inputs_and_outputs(tx, |out| {
                let out_addr = Address::from_bytes(&out.address);
                match out_addr {
                    Ok(Address::Byron(_)) => false,
                    Ok(Address::Shelley(s)) => s.delegation().to_vec().to_vec().eq(stake),
                    Ok(Address::Stake(s)) => s.to_vec().eq(stake),
                    _ => true,
                }
            }),
            FilterConfig::Spent(token_filter) => {
                any_inputs_and_outputs(tx, |out| token_filter.applies(&out.assets))
            }
            FilterConfig::Mint(token_filter) => token_filter.applies(&tx.mint),
            FilterConfig::Withdraw { credential } => tx
                .withdrawals
                .iter()
                .any(|w| w.reward_account == credential),
            FilterConfig::Signed { .. } => {
                /*
                if let Some(witnesses) = tx.witnesses {
                    Ok(witnesses.vkeywitness.iter().any(|vk| {
                        let mut hasher = Hasher::<224>::new();
                        hasher.input(&vk.vkey);
                        pkh == &hasher.finalize().to_vec()
                    }))
                } else {
                    Ok(true)
                }
                */
                true
            }
        }
    }
}
