#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include <fmt/format.h>

namespace duckdb {

void ReservoirChunk::Serialize(Serializer &serializer) const {
	chunk.Serialize(serializer);
}

unique_ptr<ReservoirChunk> ReservoirChunk::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<ReservoirChunk>();
	result->chunk.Deserialize(deserializer);
	return result;
}

unique_ptr<ReservoirChunk> ReservoirChunk::Copy() const {
	auto copy = make_uniq<ReservoirChunk>();
	copy->chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());

	chunk.Copy(copy->chunk);
	return copy;
}

ReservoirSample::ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_count(sample_count) {
	type = SampleType::RESERVOIR_SAMPLE;
}

ReservoirSample::ReservoirSample(idx_t sample_count, int64_t seed)
    : ReservoirSample(Allocator::DefaultAllocator(), sample_count, seed) {
}

void BaseReservoirSampling::IncreaseNumEntriesSeenTotal(idx_t count) {
	num_entries_seen_total += count;
}

BaseReservoirSampling::BaseReservoirSampling(int64_t seed) : random(seed) {
	next_index_to_sample = 0;
	min_weight_threshold = 0;
	min_weighted_entry_index = 0;
	num_entries_to_skip_b4_next_sample = 0;
	num_entries_seen_total = 0;
}

BaseReservoirSampling::BaseReservoirSampling() : BaseReservoirSampling(1) {
}

unique_ptr<BaseReservoirSampling> BaseReservoirSampling::Copy() {
	auto ret = make_uniq<BaseReservoirSampling>(1);
	ret->reservoir_weights = reservoir_weights;
	ret->next_index_to_sample = next_index_to_sample;
	ret->min_weight_threshold = min_weight_threshold;
	ret->min_weighted_entry_index = min_weighted_entry_index;
	ret->num_entries_to_skip_b4_next_sample = num_entries_to_skip_b4_next_sample;
	ret->num_entries_seen_total = num_entries_seen_total;
	return ret;
}

void BaseReservoirSampling::InitializeReservoirWeights(idx_t cur_size, idx_t sample_size) {
	//! 1: The first m items of V are inserted into R
	//! first we need to check if the reservoir already has "m" elements
	//! 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
	//! we then define the threshold to enter the reservoir T_w as the minimum key of R
	//! we use a priority queue to extract the minimum key in O(1) time
	if (cur_size == sample_size) {
		//! 2. For each item vi ∈ R: Calculate a key ki = random(0, 1)
		//! we then define the threshold to enter the reservoir T_w as the minimum key of R
		//! we use a priority queue to extract the minimum key in O(1) time
		for (idx_t i = 0; i < sample_size; i++) {
			double k_i = random.NextRandom();
			reservoir_weights.emplace(-k_i, i);
		}
		SetNextEntry();
	}
}

void BaseReservoirSampling::SetNextEntry() {
	//! 4. Let r = random(0, 1) and Xw = log(r) / log(T_w)
	auto &min_key = reservoir_weights.top();
	double t_w = -min_key.first;
	double r = random.NextRandom();
	double x_w = log(r) / log(t_w);
	//! 5. From the current item vc skip items until item vi , such that:
	//! 6. wc +wc+1 +···+wi−1 < Xw <= wc +wc+1 +···+wi−1 +wi
	//! since all our weights are 1 (uniform sampling), we can just determine the amount of elements to skip
	min_weight_threshold = t_w;
	min_weighted_entry_index = min_key.second;
	next_index_to_sample = MaxValue<idx_t>(1, idx_t(round(x_w)));
	num_entries_to_skip_b4_next_sample = 0;
}

void BaseReservoirSampling::ReplaceElementWithIndex(duckdb::idx_t entry_index, double with_weight) {

	double r2 = with_weight;
	//! now we insert the new weight into the reservoir
	reservoir_weights.emplace(std::make_pair(-r2, entry_index));
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

void BaseReservoirSampling::ReplaceElement(double with_weight) {
	//! replace the entry in the reservoir
	//! pop the minimum entry
	reservoir_weights.pop();
	//! now update the reservoir
	//! 8. Let tw = Tw i , r2 = random(tw,1) and vi’s key: ki = (r2)1/wi
	//! 9. The new threshold Tw is the new minimum key of R
	//! we generate a random number between (min_weight_threshold, 1)
	double r2 = random.NextRandom(min_weight_threshold, 1);

	//! if we are merging two reservoir samples use the weight passed
	if (with_weight >= 0) {
		r2 = with_weight;
	}
	//! now we insert the new weight into the reservoir
	reservoir_weights.emplace(-r2, min_weighted_entry_index);
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

std::pair<double, idx_t> BlockingSample::PopFromWeightQueue() {
	D_ASSERT(base_reservoir_sample && !base_reservoir_sample->reservoir_weights.empty());
	auto ret = base_reservoir_sample->reservoir_weights.top();
	base_reservoir_sample->reservoir_weights.pop();

	if (base_reservoir_sample->reservoir_weights.empty()) {
		// 1 is maximum weight
		base_reservoir_sample->min_weight_threshold = 1;
		return ret;
	}
	auto &min_key = base_reservoir_sample->reservoir_weights.top();
	base_reservoir_sample->min_weight_threshold = -min_key.first;
	return ret;
}

double BlockingSample::GetMinWeightThreshold() {
	return base_reservoir_sample->min_weight_threshold;
}

idx_t BlockingSample::GetPriorityQueueSize() {
	return base_reservoir_sample->reservoir_weights.size();
}

void BlockingSample::Destroy() {
	destroyed = true;
}

void ReservoirSample::AddToReservoir(DataChunk &input) {
	if (sample_count == 0 || destroyed) {
		// sample count is 0, means no samples were requested
		// destroyed means the original table has been altered and the changes have not yet
		// been reflected within the sample reservoir. So we also don't add anything
		return;
	}
	base_reservoir_sample->num_entries_seen_total += input.size();
	// Input: A population V of n weighted items
	// Output: A reservoir R with a size m
	// 1: The first m items of V are inserted into R
	// first we need to check if the reservoir already has "m" elements
	if (!reservoir_chunk || Chunk().size() < sample_count) {
		if (FillReservoir(input) == 0) {
			// entire chunk was consumed by reservoir
			return;
		}
	}
	D_ASSERT(reservoir_chunk);
	D_ASSERT(Chunk().size() == sample_count);
	// Initialize the weights if we have collected sample_count rows and weights have not been initialized
	if (Chunk().size() == sample_count && GetPriorityQueueSize() == 0) {
		base_reservoir_sample->InitializeReservoirWeights(Chunk().size(), sample_count);
	}
	// find the position of next_index_to_sample relative to number of seen entries (num_entries_to_skip_b4_next_sample)
	idx_t remaining = input.size();
	idx_t base_offset = 0;
	while (true) {
		idx_t offset =
		    base_reservoir_sample->next_index_to_sample - base_reservoir_sample->num_entries_to_skip_b4_next_sample;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample->num_entries_to_skip_b4_next_sample += remaining;
			return;
		}
		// in this chunk! replace the element
		ReplaceElement(input, base_offset + offset);
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

unique_ptr<BlockingSample> ReservoirSample::Copy() const {
	auto ret = make_uniq<ReservoirSample>(Allocator::DefaultAllocator(), sample_count);
	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	ret->reservoir_chunk = nullptr;
	ret->destroyed = destroyed;
	if (reservoir_chunk) {
		ret->reservoir_chunk = reservoir_chunk->Copy();
	}
	unique_ptr<BlockingSample> base_ret = std::move(ret);
	return base_ret;
}

struct ReplacementHelper {
	bool exists;
	std::pair<double, idx_t> pair;
};

void ReservoirSample::CombineMerge(vector<unique_ptr<ReservoirSample>> small_samples) {
	D_ASSERT(!small_samples.empty());
	D_ASSERT(!reservoir_chunk);
	CreateReservoirChunk(small_samples.at(0)->GetChunk()->GetTypes());

	Chunk().SetCardinality(sample_count);

	// We need to pop samples until we have the highest this.sample_count samples
	// left among all small samples.
	vector<ReplacementHelper> lowest_weighted_samples;

	// set all the replacement candidates
	idx_t num_entries_seen_total = 0;
	idx_t num_entries_to_skip_b4_next_sample = 0;
	for (const auto &small_sample : small_samples) {
		const auto candidate = small_sample->PopFromWeightQueue();
		const ReplacementHelper help {true, candidate};
		lowest_weighted_samples.push_back(help);
		num_entries_seen_total += small_sample->base_reservoir_sample->num_entries_seen_total;
		num_entries_to_skip_b4_next_sample += small_sample->base_reservoir_sample->num_entries_to_skip_b4_next_sample;
	}
	base_reservoir_sample->num_entries_seen_total = num_entries_seen_total;
	base_reservoir_sample->num_entries_to_skip_b4_next_sample = num_entries_to_skip_b4_next_sample;

	idx_t remaining_sample_weights = 0;
	for (const auto &small_sample : small_samples) {
		remaining_sample_weights += small_sample->GetPriorityQueueSize();
	}

	if (remaining_sample_weights < sample_count) {
		// we need everything, so push the weights from the lowest weighted samples back on
		for (idx_t i = 0; i < small_samples.size(); i++) {
			small_samples.at(i)->base_reservoir_sample->reservoir_weights.emplace(lowest_weighted_samples.at(i).pair);
			remaining_sample_weights += 1;
		}
	}

	// sample weights are stored so that the lowest weight is at the top of the priority queue
	// always. So we pop until the samples collected is equal to out sample count
	while (remaining_sample_weights > sample_count) {
		// first find the candidate with the highest weight
		double cur_highest = NumericLimits<double>::Maximum();
		idx_t highest_ind = 0;

		// find the cadidate with the highest weight
		for (idx_t i = 0; i < lowest_weighted_samples.size(); i++) {
			if (!lowest_weighted_samples.at(i).exists) {
				continue;
			}
			if (cur_highest > lowest_weighted_samples.at(i).pair.first) {
				cur_highest = lowest_weighted_samples.at(i).pair.first;
				highest_ind = i;
			}
		}

		// replace lowest_weighted_samples pop the candidate with the highest weight
		if (small_samples.at(highest_ind)->GetPriorityQueueSize() != 0) {
			const auto candidate = small_samples.at(highest_ind)->PopFromWeightQueue();
			const ReplacementHelper help {true, candidate};
			lowest_weighted_samples.at(highest_ind) = help;
			remaining_sample_weights -= 1;
		} else {
			const ReplacementHelper help {false, std::make_pair(0, 0)};
			lowest_weighted_samples.at(highest_ind) = help;
		}
	}
#ifdef DEBUG
	idx_t weights_left = 0;
	for (const auto &small_sample : small_samples) {
		weights_left += small_sample->GetPriorityQueueSize();
	}
	D_ASSERT(weights_left == sample_count);
#endif
	D_ASSERT(remaining_sample_weights == sample_count);
	// If we combine all the small samples, we have sample_count samples.
	// so now we just push all of those samples into our reservoir sample
	idx_t index_to_replace = 0;
	for (idx_t i = 0; i < small_samples.size(); i++) {
		auto &sample_to_empty = small_samples.at(i);
		while (sample_to_empty->GetPriorityQueueSize() > 0) {
			// replace the element with the lowest index
			auto sample_pair_other = sample_to_empty->PopFromWeightQueue();
			D_ASSERT(sample_pair_other.first < 0);
			ReplaceElement(index_to_replace, sample_to_empty->Chunk(), sample_pair_other.second,
			               -sample_pair_other.first);
			index_to_replace += 1;
		}
	}
	D_ASSERT(index_to_replace == remaining_sample_weights);
}

void ReservoirSample::Merge(unique_ptr<BlockingSample> other) {
	// do not merge destroyed samples.
	if (destroyed || other->destroyed) {
		Destroy();
		return;
	}

	if (other->type == SampleType::RESERVOIR_PERCENTAGE_SAMPLE && reservoir_chunk != nullptr) {
		// convert the percentage sample into a reservoir sample and merge those two.
		auto &other_percentage_sample = other->Cast<ReservoirSamplePercentage>();
		other_percentage_sample.Finalize();
		auto other_sample_count = other_percentage_sample.NumSamplesCollected();
		auto converted_percentage_sample = other_percentage_sample.ConvertToFixedReservoirSample(other_sample_count);
		return Merge(std::move(converted_percentage_sample));
	}

	auto reservoir_other = &other->Cast<ReservoirSample>();

	// There are four combinations for reservoir state

	// 1. This reservoir chunk has not yet been initialized.
	if (reservoir_chunk == nullptr) {
		if (sample_count != reservoir_other->sample_count) {
			// TODO: just take the highest sample_count weights from reservoir_other
			throw InternalException("Need to implement this first");
		}
		// take ownership of the reservoir_others sample
		base_reservoir_sample = std::move(other->base_reservoir_sample);
		reservoir_chunk = std::move(reservoir_other->reservoir_chunk);
		return;
	}

	if (reservoir_other->reservoir_chunk == nullptr) {
		return;
	}

	// 2. Both do not have full reservoir chunks
	if (Chunk().size() + reservoir_other->Chunk().size() < sample_count) {
		// the sum of both reservoir chunk sizes is less than sample count.
		// both samples have not yet thrown away tuples or assigned weights to tuples in the sample
		// Therefore we can just grab chunks from other and add them to this sample.
		// all logic to assign weights will automatically be handled in AddReservoir.
		auto chunk = reservoir_other->GetChunkAndShrink();
		while (chunk) {
			AddToReservoir(*chunk);
			chunk = reservoir_other->GetChunkAndShrink();
		}
		return;
	}

	// 3. Only one has a full reservoir chunk
	// merge the one that has not yet been filled into the full one.
	// The one not yet filled has not skipped any tuples yet, so we are not biased to it's sample when
	// using AddToReservoir.
	if (Chunk().size() + reservoir_other->Chunk().size() < sample_count + reservoir_other->sample_count) {
		// one of the samples is full, but not the other.
		if (GetPriorityQueueSize() == sample_count) {
			auto chunk = reservoir_other->GetChunkAndShrink();
			while (chunk) {
				AddToReservoir(*chunk);
				chunk = reservoir_other->GetChunkAndShrink();
			}
		} else {
			// other is full
			// grab chunks from this to fill other
			D_ASSERT(reservoir_other->GetPriorityQueueSize() == sample_count);
			auto chunk = GetChunkAndShrink();
			while (chunk) {
				reservoir_other->AddToReservoir(*chunk);
				chunk = GetChunkAndShrink();
			}
			// now take ownership of the sample of other.
			base_reservoir_sample = std::move(other->base_reservoir_sample);
			reservoir_chunk = std::move(reservoir_other->reservoir_chunk);
		}
		return;
	}

	//  4. this and other both have full reservoirs where each index in the sample has a weight
	//  Each reservoir has sample_count rows/tuples. We only want to keep the highest weighted samples
	//	so we remove sample_count tuples/rows from the reservoirs that have the lowest weights
	//	Then push the rest of the samples from other into this, using the weights the tuples had in other
	idx_t pop_count = Chunk().size() + reservoir_other->Chunk().size() - sample_count;
	D_ASSERT(pop_count == reservoir_other->sample_count);

	// store indexes that need to be replaced in this
	// store weights for new values that will be replacing old values
	vector<idx_t> replaceable_indexes;
	auto min_weight_threshold_this = GetMinWeightThreshold();
	auto min_weight_threshold_other = reservoir_other->GetMinWeightThreshold();
	for (idx_t i = 0; i < pop_count; i++) {
		D_ASSERT(GetPriorityQueueSize() + reservoir_other->GetPriorityQueueSize() >= sample_count);
		if (min_weight_threshold_this < min_weight_threshold_other) {
			auto top = PopFromWeightQueue();
			// top.second holds the index of the replaceable tuple
			replaceable_indexes.push_back(top.second);
		} else {
			reservoir_other->PopFromWeightQueue();
		}
		min_weight_threshold_this = GetMinWeightThreshold();
		min_weight_threshold_other = reservoir_other->GetMinWeightThreshold();
	}

	while (!replaceable_indexes.empty()) {
		auto top_other = reservoir_other->PopFromWeightQueue();
		auto index_to_replace = replaceable_indexes.back();
		ReplaceElement(index_to_replace, reservoir_other->Chunk(), top_other.second, -top_other.first);
		replaceable_indexes.pop_back();
	}

	D_ASSERT(GetPriorityQueueSize() == sample_count);
}

unique_ptr<DataChunk> ReservoirSample::GetChunk(idx_t offset) {
	if (destroyed || !reservoir_chunk || Chunk().size() == 0 || offset >= Chunk().size()) {
		return nullptr;
	}
	auto ret = make_uniq<DataChunk>();
	idx_t ret_chunk_size = FIXED_SAMPLE_SIZE;
	if (offset + FIXED_SAMPLE_SIZE > Chunk().size()) {
		ret_chunk_size = Chunk().size() - offset;
	}
	auto reservoir_types = Chunk().GetTypes();
	SelectionVector sel(FIXED_SAMPLE_SIZE);
	for (idx_t i = offset; i < offset + ret_chunk_size; i++) {
		sel.set_index(i - offset, i);
	}
	ret->Initialize(allocator, reservoir_types.begin(), reservoir_types.end(), FIXED_SAMPLE_SIZE);
	ret->Slice(Chunk(), sel, FIXED_SAMPLE_SIZE);
	ret->SetCardinality(ret_chunk_size);
	return ret;
}

unique_ptr<DataChunk> ReservoirSample::GetChunkAndShrink() {
	if (!reservoir_chunk || Chunk().size() == 0 || destroyed) {
		return nullptr;
	}
	if (Chunk().size() > FIXED_SAMPLE_SIZE) {
		// get from the back
		auto ret = make_uniq<DataChunk>();
		auto samples_remaining = Chunk().size() - FIXED_SAMPLE_SIZE;
		auto reservoir_types = Chunk().GetTypes();
		SelectionVector sel(FIXED_SAMPLE_SIZE);
		for (idx_t i = samples_remaining; i < Chunk().size(); i++) {
			sel.set_index(i - samples_remaining, i);
		}
		ret->Initialize(allocator, reservoir_types.begin(), reservoir_types.end(), FIXED_SAMPLE_SIZE);
		ret->Slice(Chunk(), sel, FIXED_SAMPLE_SIZE);
		ret->SetCardinality(FIXED_SAMPLE_SIZE);
		// reduce capacity and cardinality of the sample data chunk
		Chunk().SetCardinality(samples_remaining);
		return ret;
	}
	auto ret = make_uniq<DataChunk>();
	ret->Initialize(allocator, Chunk().GetTypes());
	Chunk().Copy(*ret);
	reservoir_chunk = nullptr;
	return ret;
}

void ReservoirSample::Destroy() {
	BlockingSample::Destroy();
	reservoir_chunk = nullptr;
}

void ReservoirSample::ReplaceElement(DataChunk &input, idx_t index_in_chunk, double with_weight) {
	// replace the entry in the reservoir with Input[index_in_chunk]
	// If index_in_self_chunk is provided, then the
	// 8. The item in R with the minimum key is replaced by item vi
	D_ASSERT(input.ColumnCount() == Chunk().ColumnCount());
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		Chunk().SetValue(col_idx, base_reservoir_sample->min_weighted_entry_index,
		                 input.GetValue(col_idx, index_in_chunk));
	}
	base_reservoir_sample->ReplaceElement(with_weight);
}

void ReservoirSample::ReplaceElement(idx_t reservoir_chunk_index, DataChunk &input, idx_t index_in_input_chunk,
                                     double with_weight) {
	// replace the entry in the reservoir with Input[index_in_chunk]
	// If index_in_self_chunk is provided, then the
	// 8. The item in R with the minimum key is replaced by item vi
	D_ASSERT(input.ColumnCount() == Chunk().ColumnCount());
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		Chunk().SetValue(col_idx, reservoir_chunk_index, input.GetValue(col_idx, index_in_input_chunk));
	}
	base_reservoir_sample->ReplaceElementWithIndex(reservoir_chunk_index, with_weight);
}

void ReservoirSample::CreateReservoirChunk(const vector<LogicalType> &types) {
	reservoir_chunk = make_uniq<ReservoirChunk>();
	Chunk().Initialize(allocator, types, sample_count);
	for (idx_t col_idx = 0; col_idx < Chunk().ColumnCount(); col_idx++) {
		FlatVector::Validity(Chunk().data[col_idx]).Initialize(sample_count);
	}
}

idx_t ReservoirSample::FillReservoir(DataChunk &input) {
	idx_t chunk_count = input.size();
	input.Flatten();
	auto num_added_samples = reservoir_chunk ? Chunk().size() : 0;
	D_ASSERT(num_added_samples <= sample_count);

	// required count is what we still need to add to the reservoir
	idx_t required_count;
	if (num_added_samples + chunk_count >= sample_count) {
		// have to limit the count of the chunk
		required_count = sample_count - num_added_samples;
	} else {
		// we copy the entire chunk
		required_count = chunk_count;
	}
	input.SetCardinality(required_count);

	// initialize the reservoir
	if (!reservoir_chunk) {
		CreateReservoirChunk(input.GetTypes());
	}
	Chunk().Append(input, false, nullptr, required_count);
	if (num_added_samples + required_count >= sample_count && GetPriorityQueueSize() == 0) {
		base_reservoir_sample->InitializeReservoirWeights(Chunk().size(), sample_count);
	}

	num_added_samples += required_count;
	Chunk().SetCardinality(num_added_samples);
	// check if there are still elements remaining in the Input data chunk that should be
	// randomly sampled and potentially added. This happens if we are on a boundary
	// for example, input.size() is 1024, but our sample size is 10
	if (required_count == chunk_count) {
		// we are done here
		return 0;
	}
	// we still need to process a part of the chunk
	// create a selection vector of the remaining elements
	SelectionVector sel(FIXED_SAMPLE_SIZE);
	for (idx_t i = required_count; i < chunk_count; i++) {
		sel.set_index(i - required_count, i);
	}
	// slice the input vector and continue
	input.Slice(sel, chunk_count - required_count);
	return input.size();
}

idx_t ReservoirSample::NumSamplesCollected() {
	auto samples = GetPriorityQueueSize();
	return samples == 0 && reservoir_chunk ? Chunk().size() : samples;
}

DataChunk &ReservoirSample::Chunk() {
	D_ASSERT(reservoir_chunk);
	return reservoir_chunk->chunk;
}

void ReservoirSample::Finalize() {
	return;
}

ReservoirSamplePercentage::ReservoirSamplePercentage(double percentage, int64_t seed, idx_t reservoir_sample_size)
    : BlockingSample(seed), allocator(Allocator::DefaultAllocator()), sample_percentage(percentage / 100.0),
      reservoir_sample_size(reservoir_sample_size), current_count(0), is_finalized(false) {
	current_sample =
	    make_uniq<ReservoirSample>(allocator, reservoir_sample_size, base_reservoir_sample->random.NextRandomInteger());
	type = SampleType::RESERVOIR_PERCENTAGE_SAMPLE;
}

ReservoirSamplePercentage::ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_percentage(percentage / 100.0), current_count(0),
      is_finalized(false) {
	reservoir_sample_size = (idx_t)(sample_percentage * RESERVOIR_THRESHOLD);
	current_sample =
	    make_uniq<ReservoirSample>(allocator, reservoir_sample_size, base_reservoir_sample->random.NextRandomInteger());
	type = SampleType::RESERVOIR_PERCENTAGE_SAMPLE;
}

ReservoirSamplePercentage::ReservoirSamplePercentage(double percentage, int64_t seed)
    : ReservoirSamplePercentage(Allocator::DefaultAllocator(), percentage, seed) {
}

void ReservoirSamplePercentage::AddToReservoir(DataChunk &input) {
	base_reservoir_sample->num_entries_seen_total += input.size();
	if (current_count + input.size() > RESERVOIR_THRESHOLD) {
		// we don't have enough space in our current reservoir
		// first check what we still need to append to the current sample
		idx_t append_to_current_sample_count = RESERVOIR_THRESHOLD - current_count;
		idx_t append_to_next_sample = input.size() - append_to_current_sample_count;
		if (append_to_current_sample_count > 0) {
			// we have elements remaining, first add them to the current sample
			if (append_to_next_sample > 0) {
				// we need to also add to the next sample
				DataChunk new_chunk;
				new_chunk.InitializeEmpty(input.GetTypes());
				new_chunk.Slice(input, *FlatVector::IncrementalSelectionVector(), append_to_current_sample_count);
				new_chunk.Flatten();
				current_sample->AddToReservoir(new_chunk);
			} else {
				input.Flatten();
				input.SetCardinality(append_to_current_sample_count);
				current_sample->AddToReservoir(input);
			}
		}
		if (append_to_next_sample > 0) {
			// slice the input for the remainder
			SelectionVector sel(append_to_next_sample);
			for (idx_t i = append_to_current_sample_count; i < append_to_next_sample + append_to_current_sample_count;
			     i++) {
				sel.set_index(i - append_to_current_sample_count, i);
			}
			input.Slice(sel, append_to_next_sample);
		}
		// now our first sample is filled: append it to the set of finished samples
		finished_samples.push_back(std::move(current_sample));

		// allocate a new sample, and potentially add the remainder of the current input to that sample
		current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size,
		                                            base_reservoir_sample->random.NextRandomInteger());
		if (append_to_next_sample > 0) {
			current_sample->AddToReservoir(input);
		}
		current_count = append_to_next_sample;
	} else {
		// we can just append to the current sample
		current_count += input.size();
		current_sample->AddToReservoir(input);
	}
}

void ReservoirSamplePercentage::FromReservoirSample(unique_ptr<ReservoirSample> other) {
	// we add tuples from the the reservoir sample
	base_reservoir_sample = other->base_reservoir_sample->Copy();
	finished_samples.push_back(std::move(other));
}

void ReservoirSamplePercentage::Merge(unique_ptr<BlockingSample> other) {
	if (destroyed || other->destroyed) {
		Destroy();
		return;
	}
	// just merge. it must to up to the calling function to convert the percentage sample
	// into a block sample.
	if (other->type != SampleType::RESERVOIR_PERCENTAGE_SAMPLE) {
		throw InternalException(string("You should never be merging a reservoir sample into ") +
		                        string("a reservoir percentage sample. Or you just don't know what you are doing"));
	}
	auto &other_percentage_sample = other->Cast<ReservoirSamplePercentage>();

	// first add the finished samples from other if they exist.
	for (auto &finished_sample : other_percentage_sample.finished_samples) {
		finished_samples.push_back(std::move(finished_sample));
	}

	// now merge the current samples.
	current_sample->Merge(std::move(other_percentage_sample.current_sample));
	current_count += other_percentage_sample.current_count;
}

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunk(idx_t offset) {
	if (!is_finalized) {
		Finalize();
	}
	if (NumSamplesCollected() > FIXED_SAMPLE_SIZE) {
		throw InternalException("Calling GetChunk() on reservoir Sample with more than standard vector size samples");
	}
	idx_t finished_sample_index = 0;
	bool can_skip_finished_sample = true;
	while (can_skip_finished_sample && finished_sample_index < finished_samples.size()) {
		auto finished_sample_count = finished_samples.at(finished_sample_index)->NumSamplesCollected();
		if (offset >= finished_sample_count) {
			offset -= finished_sample_count;
			finished_sample_index += 1;
		} else {
			can_skip_finished_sample = false;
		}
	}
	if (finished_sample_index >= finished_samples.size()) {
		return nullptr;
	}
	return finished_samples.at(finished_sample_index)->GetChunk(offset);
}

idx_t ReservoirSamplePercentage::NumSamplesCollected() {
	if (destroyed) {
		return 0;
	}
	idx_t samples_collected = 0;
	for (auto &finished_sample : finished_samples) {
		samples_collected += finished_sample->NumSamplesCollected();
	}
	if (!is_finalized && current_sample) {
		// Sometimes a percentage sample can overcollect. When finalize is called the
		// percentage becomes accurate, however.
		samples_collected += idx_t(current_sample->base_reservoir_sample->num_entries_seen_total * sample_percentage);
	}
	return samples_collected;
}

unique_ptr<BlockingSample> ReservoirSamplePercentage::Copy() const {
	auto ret = make_uniq<ReservoirSamplePercentage>(Allocator::DefaultAllocator(), (sample_percentage * 100), 1);
	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	auto cur_sample_copy = current_sample->Copy();
	D_ASSERT(current_sample->type == SampleType::RESERVOIR_SAMPLE);
	ret->current_sample = duckdb::unique_ptr_cast<BlockingSample, ReservoirSample>(current_sample->Copy());

	for (auto &finished_sample : finished_samples) {
		ret->finished_samples.push_back(
		    duckdb::unique_ptr_cast<BlockingSample, ReservoirSample>(finished_sample->Copy()));
	}
	ret->current_count = current_count;
	ret->is_finalized = is_finalized;
	ret->reservoir_sample_size = reservoir_sample_size;
	return unique_ptr_cast<ReservoirSamplePercentage, BlockingSample>(std::move(ret));
}

unique_ptr<ReservoirSample> ReservoirSamplePercentage::ConvertToFixedReservoirSample(idx_t sample_count) {
	if (!is_finalized) {
		Finalize();
	}

	// This function should never be called if the number of samples collected is smaller than the sample count
	D_ASSERT(NumSamplesCollected() >= sample_count);
	// Make sure that the reservoir sample percentage more than sample count samples.
	auto reservoir_sample = make_uniq<ReservoirSample>(allocator, sample_count, 1);
	// insert the first chunk from the percentage sample as if these are all first time
	if (reservoir_sample->destroyed || sample_count == 0) {
		return reservoir_sample;
	}
	// if there is a single finished_sample with the same sample count, just merge all finished samples into the sample
	// count
	idx_t finished_sample_index = 0;
	// if the sample counts of our finished samples and our desired Reservoir Sample do not line up
	// then we need to make sure we can convert them properly
	vector<unique_ptr<ReservoirSample>> mini_small_samples;
	idx_t finished_samples_count = 0;
	finished_sample_index = 0;
	for (; finished_sample_index < finished_samples.size(); finished_sample_index++) {
		auto &finished_sample = finished_samples.at(finished_sample_index);
		if (finished_sample->sample_count != sample_count) {
			auto num_samples_collected = finished_sample->NumSamplesCollected();
			if (num_samples_collected == 0) {
				continue;
			}
			if (num_samples_collected < finished_sample->sample_count) {
				// finished sample has not yet assigned weights.
				finished_sample->base_reservoir_sample->InitializeReservoirWeights(num_samples_collected,
				                                                                   num_samples_collected);
			}
			finished_samples_count += num_samples_collected;
			mini_small_samples.push_back(std::move(finished_samples.at(finished_sample_index)));
		}
		// you have enough of the smaller finished samples. Now you can combine them
		// and merge into a larger blocking sample.
		if (finished_samples_count >= sample_count) {
			reservoir_sample->CombineMerge(std::move(mini_small_samples));
			break;
		}
	}
	finished_sample_index++;
	// if the smaller samples have been merged, you can just merge the other finished samples now
	for (; finished_sample_index < finished_samples.size(); finished_sample_index++) {
		reservoir_sample->Merge(std::move(finished_samples.at(finished_sample_index)));
	}

	return reservoir_sample;
}

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunkAndShrink() {
	if (!is_finalized) {
		Finalize();
	}
	while (!finished_samples.empty()) {
		auto &front = finished_samples.front();
		auto chunk = front->GetChunkAndShrink();
		if (chunk && chunk->size() > 0) {
			return chunk;
		}
		// move to the next sample
		finished_samples.erase(finished_samples.begin());
	}
	return nullptr;
}

void ReservoirSamplePercentage::Finalize() {
	// need to finalize the current sample, if any
	// we are finializing, so we are starting to return chunks. Our last chunk has
	// sample_percentage * RESERVOIR_THRESHOLD entries that hold samples.
	// if our current count is less than the sample_percentage * RESERVOIR_THRESHOLD
	// then we have sampled too much for the current_sample and we need to redo the sample
	// otherwise we can just push the current sample back
	// Imagine sampling 70% of 100 rows (so 70 rows). We allocate sample_percentage * RESERVOIR_THRESHOLD
	// -----------------------------------------
	auto sampled_more_than_required =
	    current_count > sample_percentage * RESERVOIR_THRESHOLD || finished_samples.empty();
	if (current_count > 0 && sampled_more_than_required) {
		// create a new sample
		auto new_sample_size = idx_t(round(sample_percentage * current_count));
		auto new_sample =
		    make_uniq<ReservoirSample>(allocator, new_sample_size, base_reservoir_sample->random.NextRandomInteger());
		while (true) {
			auto chunk = current_sample->GetChunkAndShrink();
			if (!chunk || chunk->size() == 0) {
				break;
			}
			new_sample->AddToReservoir(*chunk);
		}
		finished_samples.push_back(std::move(new_sample));
	} else {
		finished_samples.push_back(std::move(current_sample));
	}
	// when finalizing, current_sample is null. All samples are now in finished samples.
	current_sample = nullptr;
	is_finalized = true;
}

// serialize/deserialize code.

unique_ptr<BlockingSample>
BlockingSample::MaybeConvertReservoirToPercentageResevoir(unique_ptr<BlockingSample> sample) {
	if (sample->type != SampleType::RESERVOIR_SAMPLE) {
		return std::move(sample);
	}
	auto reservoir_sample = unique_ptr_cast<BlockingSample, ReservoirSample>(std::move(sample));
	if (reservoir_sample->GetPriorityQueueSize() == 0) {
		// TODO: what has happened here?
		return std::move(sample);
	}
	auto top_weight = reservoir_sample->base_reservoir_sample->reservoir_weights.top();
	if (top_weight.first != NumericLimits<double>::Maximum()) {
		D_ASSERT(top_weight.first < 0);
		// the top weight is a valid weight, so this is a valid reservoir sample
		return std::move(reservoir_sample);
	}
	// the top weight is a impossible weight value (weights are only negative), this tells us
	// the sample was a percentage reservoir sample to start, so we pop the fake value and convert the
	// sample to a percentage sample
	auto sample_percentage = top_weight.second;
	reservoir_sample->base_reservoir_sample->reservoir_weights.pop();
	D_ASSERT(reservoir_sample->NumSamplesCollected() == reservoir_sample->GetPriorityQueueSize());
	// if we have less than a standard vector size and there are no weights, this was almost certainly a serialized
	// percentage sample. the only time we deserialize reservoir samples is because they are an actual sample. because
	// of a dumb mistake I (Tom Ebergen) made, we serialize smaller percentage samples as normal reservoir samples we
	// can recreate the percentage sample here
	auto percentage_sample =
	    duckdb::unique_ptr<ReservoirSamplePercentage>(new ReservoirSamplePercentage(sample_percentage));
	percentage_sample->FromReservoirSample(std::move(reservoir_sample));
	// the base_reservoir_weights are deserialized during whatever the calling class is doing.
	// We need these base_reservoir_weights before we create the percentage sample, because they
	// are important. The deserializing/Conversion needs to move somewhere else that can check
	// if the sample needs to be converted before it is appended to or read.
	return std::move(percentage_sample);
}

void BlockingSample::Serialize(Serializer &serializer) const {
	serializer.WritePropertyWithDefault<unique_ptr<BaseReservoirSampling>>(100, "base_reservoir_sample",
	                                                                       base_reservoir_sample);
	serializer.WriteProperty<SampleType>(101, "type", type);
	serializer.WritePropertyWithDefault<bool>(102, "destroyed", destroyed);
}

unique_ptr<BlockingSample> BlockingSample::Deserialize(Deserializer &deserializer) {
	auto base_reservoir_sample =
	    deserializer.ReadPropertyWithDefault<unique_ptr<BaseReservoirSampling>>(100, "base_reservoir_sample");
	auto type = deserializer.ReadProperty<SampleType>(101, "type");
	auto destroyed = deserializer.ReadPropertyWithDefault<bool>(102, "destroyed");
	unique_ptr<BlockingSample> result;
	switch (type) {
	case SampleType::RESERVOIR_PERCENTAGE_SAMPLE:
		result = ReservoirSamplePercentage::Deserialize(deserializer);
		break;
	case SampleType::RESERVOIR_SAMPLE:
		result = ReservoirSample::Deserialize(deserializer);
		break;
	default:
		throw SerializationException("Unsupported type for deserialization of BlockingSample!");
	}
	result->base_reservoir_sample = std::move(base_reservoir_sample);
	result->destroyed = destroyed;
	auto converted_result = MaybeConvertReservoirToPercentageResevoir(std::move(result));
	return converted_result;
}

void ReservoirSample::Serialize(Serializer &serializer) const {
	BlockingSample::Serialize(serializer);
	serializer.WritePropertyWithDefault<idx_t>(200, "sample_count", sample_count);
	serializer.WritePropertyWithDefault<unique_ptr<ReservoirChunk>>(201, "reservoir_chunk", reservoir_chunk);
}

unique_ptr<BlockingSample> ReservoirSample::Deserialize(Deserializer &deserializer) {
	auto sample_count = deserializer.ReadPropertyWithDefault<idx_t>(200, "sample_count");
	auto result = duckdb::unique_ptr<ReservoirSample>(new ReservoirSample(sample_count));
	deserializer.ReadPropertyWithDefault<unique_ptr<ReservoirChunk>>(201, "reservoir_chunk", result->reservoir_chunk);
	return std::move(result);
}

void ReservoirSamplePercentage::Serialize(Serializer &serializer) const {
	auto copy = Copy();
	auto &copy_percentage = copy->Cast<ReservoirSamplePercentage>();
	auto copy_as_reservoir_sample = copy_percentage.ConvertToFixedReservoirSample(copy->NumSamplesCollected());
	copy_as_reservoir_sample->base_reservoir_sample->reservoir_weights.emplace(
	    std::make_pair(NumericLimits<double>::Maximum(), idx_t(copy_percentage.sample_percentage * 100)));
	copy_as_reservoir_sample->Serialize(serializer);
}

unique_ptr<BlockingSample> ReservoirSamplePercentage::Deserialize(Deserializer &deserializer) {
	auto sample_percentage = deserializer.ReadProperty<double>(200, "sample_percentage");
	auto result = duckdb::unique_ptr<ReservoirSamplePercentage>(new ReservoirSamplePercentage(sample_percentage));
	deserializer.ReadPropertyWithDefault<idx_t>(201, "reservoir_sample_size", result->reservoir_sample_size);
	return std::move(result);
}

} // namespace duckdb
