raise NotImplementedError("This script is not implemented yet.")


# How to apply bid / offer multipliers to the marginal costs, now that fuel/co2/opex have been split
# Seperate RoE into one zone per interconnector endpoint and keep the marginal cost as fully dispatchable generator + a load to absorb the scheduled dispatch (peek at current implementation for details about this -> infinite store; what are the costs for withdrawing/storing?)
# Prices for bid/offer on interconnectors originally based on lowest prices in any of RoE; we do not collapse RoE, i.e. we should keep the marginal prices of the second port of the interconnector instead, i.e. recalculate the marginal cost
# Limitation: GBNI/IE might not have sufficient resources for full redispatch reverse flow, but we assume they have (like we assume it for RoE)
# DSR generators with costs are already included, make sure they align with load shedding somehow


# Rule-wise:
# after solve_unconstrained
# extract the interconnector_bid_offer_profile (only dependent on the dispatch results, all others are independent)
# then prepare the redispatch and then solve the redispatch


# Bid-offer multipliers are per carrier of the GB dispatch model. As such need to be mapped as well to the new carrier names
