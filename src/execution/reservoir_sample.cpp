#include "duckdb/execution/reservoir_sample.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/types/data_chunk.hpp"

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

void BaseReservoirSampling::InitializeReservoirWeights(idx_t cur_size, idx_t sample_size, idx_t index_offset) {
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
			idx_t index = i + index_offset;
			double k_i = random.NextRandom();
			reservoir_weights.emplace(-k_i, index);
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

void ReservoirSample::Merge(unique_ptr<BlockingSample> other) {
	throw InternalException("resevoir sample merge called");
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

unique_ptr<IngestionSample> ReservoirSample::ConvertToIngestionSample() {
	auto ingestion_sample = make_uniq<IngestionSample>(sample_count);

	// first add the chunks
	auto chunk = GetChunkAndShrink();
	while (chunk) {
		ingestion_sample->AddToReservoir(*chunk);
		chunk = GetChunkAndShrink();
	}

	// then assign the weights
	ingestion_sample->base_reservoir_sample = std::move(base_reservoir_sample);
	return ingestion_sample;
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
	throw InternalException("reservoir sample percentage merge called");
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
	    static_cast<double>(current_count) > sample_percentage * RESERVOIR_THRESHOLD || finished_samples.empty();
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

idx_t IngestionSample::NumSamplesCollected() {
	idx_t samples_collected = 0;
	for (auto &chk : sample_chunks) {
		samples_collected += chk->size();
	}
	return samples_collected;
}

unique_ptr<DataChunk> IngestionSample::GetChunk(idx_t offset) {
	Shrink();
	D_ASSERT(sample_chunks.size() <= 1);
	auto ret = make_uniq<DataChunk>();
	if (sample_chunks.size() == 0 || destroyed) {
		return nullptr;
	}
	idx_t ret_chunk_size = FIXED_SAMPLE_SIZE;
	auto &chunk_to_copy = sample_chunks.at(0);
	if (offset + FIXED_SAMPLE_SIZE > chunk_to_copy->size()) {
		ret_chunk_size = chunk_to_copy->size() - offset;
	}
	if (ret_chunk_size == 0) {
		return nullptr;
	}
	auto reservoir_types = chunk_to_copy->GetTypes();
	SelectionVector sel(FIXED_SAMPLE_SIZE);
	for (idx_t i = offset; i < offset + ret_chunk_size; i++) {
		sel.set_index(i - offset, i);
	}
	ret->Initialize(allocator, reservoir_types.begin(), reservoir_types.end(), FIXED_SAMPLE_SIZE);
	ret->Slice(*chunk_to_copy, sel, FIXED_SAMPLE_SIZE);
	ret->SetCardinality(ret_chunk_size);
	return ret;
}

unique_ptr<DataChunk> IngestionSample::GetChunkAndShrink() {
	throw InternalException("Should never call get chunk and shrink on Ingestion Sample");
}

void IngestionSample::Shrink() {
	Verify();
	if (NumSamplesCollected() <= FIXED_SAMPLE_SIZE || sample_chunks.size() == 1) {
		// nothing to shrink, haven't collected enough samples.
		return;
	}

	if (destroyed) {
		return;
	}

	// we will only keep one sample size of samples
	idx_t num_samples_to_keep = FIXED_SAMPLE_SIZE;
	vector<std::pair<double, idx_t>> weights_indexes;
	D_ASSERT(num_samples_to_keep == base_reservoir_sample->reservoir_weights.size());
	for (idx_t i = 0; i < num_samples_to_keep; i++) {
		weights_indexes.push_back(base_reservoir_sample->reservoir_weights.top());
		base_reservoir_sample->reservoir_weights.pop();
	}

	// create one large chunk from the collected chunk samples.
	D_ASSERT(!sample_chunks.empty());
	auto &chunk_to_copy = sample_chunks[0];
	for (idx_t i = 1; i < sample_chunks.size(); i++) {
		chunk_to_copy->Append(*sample_chunks[i], true, nullptr, sample_chunks[i]->size());
	}

	// create a new sample chunk to store new samples
	auto new_sample_chunk = make_uniq<DataChunk>();
	new_sample_chunk->Initialize(Allocator::DefaultAllocator(), chunk_to_copy->GetTypes(), FIXED_SAMPLE_SIZE);
	for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(new_sample_chunk->data[col_idx]).Initialize(FIXED_SAMPLE_SIZE);
	}
	new_sample_chunk->SetCardinality(num_samples_to_keep);

	// set up selection vector to copy IngestionSample to ReservoirSample
	SelectionVector sel(num_samples_to_keep);
	// reservoir weights should be empty. We are about to construct them again with indexes in the new_sample_chunk
	D_ASSERT(base_reservoir_sample->reservoir_weights.empty());
	double max_weight = NumericLimits<double>::Minimum();
	idx_t max_weight_index = 0;
	for (idx_t i = 0; i < num_samples_to_keep; i++) {
		sel.set_index(i, weights_indexes[i].second);
		base_reservoir_sample->reservoir_weights.emplace(weights_indexes[i].first, i);
		if (max_weight < weights_indexes[i].first) {
			max_weight = weights_indexes[i].first;
			max_weight_index = i;
		}
	}
	base_reservoir_sample->min_weighted_entry_index = max_weight_index;
	base_reservoir_sample->min_weight_threshold = -max_weight;

	// perform the copy
	for (idx_t col_idx = 0; col_idx < chunk_to_copy->ColumnCount(); col_idx++) {
		VectorOperations::Copy(chunk_to_copy->data[col_idx], new_sample_chunk->data[col_idx], sel, num_samples_to_keep,
		                       0, 0);
	}

	sample_chunks.clear();
	sample_chunks.push_back(std::move(new_sample_chunk));
	Verify();
	// We should only have one sample chunk now.
	D_ASSERT(sample_chunks.size() == 1);
}

unique_ptr<BlockingSample> IngestionSample::Copy() const {
	auto ret = make_uniq<IngestionSample>(sample_count);

	ret->base_reservoir_sample = base_reservoir_sample->Copy();
	ret->destroyed = destroyed;
	if (sample_chunks.size() == 0 || destroyed) {
		return ret;
	}
	// create one large chunk from the collected chunk samples.
	// before calling copy(), shrink() must be called.
	// Shrink() cannot be called within copy since copy is const.
	D_ASSERT(sample_chunks.size() == 1);
	auto &chunk_to_copy = sample_chunks[0];

	// create a new sample chunk to store new samples
	auto new_sample_chunk = make_uniq<DataChunk>();
	new_sample_chunk->Initialize(Allocator::DefaultAllocator(), chunk_to_copy->GetTypes(), FIXED_SAMPLE_SIZE);
	for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(new_sample_chunk->data[col_idx]).Initialize(FIXED_SAMPLE_SIZE);
	}
	// copy chunk to copy into new sample chunk
	chunk_to_copy->Copy(*new_sample_chunk);
	new_sample_chunk->SetCardinality(chunk_to_copy->size());

	ret->sample_chunks.push_back(std::move(new_sample_chunk));
	// We should only have one sample chunk now.
	D_ASSERT(sample_chunks.size() == 1);

	ret->Verify();
	return ret;
}

void IngestionSample::Verify() {
	if (destroyed) {
		return;
	}
	if (NumSamplesCollected() > FIXED_SAMPLE_SIZE) {
		D_ASSERT(GetPriorityQueueSize() == FIXED_SAMPLE_SIZE);
	} else if (NumSamplesCollected() <= FIXED_SAMPLE_SIZE && GetPriorityQueueSize() > 0) {
		D_ASSERT(NumSamplesCollected() == GetPriorityQueueSize());
	}
}

void IngestionSample::Merge(unique_ptr<BlockingSample> other) {
	if (destroyed || other->destroyed) {
		Destroy();
		return;
	}

	D_ASSERT(other->type == SampleType::INGESTION_SAMPLE);
	auto &other_ingest = other->Cast<IngestionSample>();

	// other has not collected samples
	if (other_ingest.sample_chunks.empty()) {
		return;
	}

	// this has not collected samples, take over the other
	if (sample_chunks.empty()) {
		base_reservoir_sample = std::move(other->base_reservoir_sample);
		sample_chunks = std::move(other_ingest.sample_chunks);
		return;
	}

	if (GetPriorityQueueSize() == 0 && NumSamplesCollected() > 0) {
		// make sure both samples have weights
		base_reservoir_sample->InitializeReservoirWeights(NumSamplesCollected(), NumSamplesCollected());
	}

	if (other_ingest.GetPriorityQueueSize() == 0 && other_ingest.NumSamplesCollected() > 0) {
		// make sure both samples have weights
		other_ingest.base_reservoir_sample->InitializeReservoirWeights(other_ingest.NumSamplesCollected(),
		                                                               other_ingest.NumSamplesCollected());
	}

	// we know both ingestion samples have collected samples,
	// shrink both samples so merging is easier
	Shrink();
	other_ingest.Shrink();
	// make sure both ingestion samples only have 1 sample after the shrink
	D_ASSERT(other_ingest.sample_chunks.size() == 1 && sample_chunks.size() == 1);

	idx_t total_samples = NumSamplesCollected() + other_ingest.NumSamplesCollected();
	idx_t num_samples_to_keep = MinValue<idx_t>(FIXED_SAMPLE_SIZE, total_samples);

	Verify();
	if (total_samples == 2049) {
		auto break_here = 0;
	}
	// if there are more than FIXED_SAMPLE_SIZE samples, we want to keep only the
	// highest weighted FIXED_SAMPLE_SIZE samples
	for (idx_t i = num_samples_to_keep; i < total_samples; i++) {
		auto min_weight_this = base_reservoir_sample->min_weight_threshold;
		auto min_weight_other = other_ingest.base_reservoir_sample->min_weight_threshold;
		// weights are stored as negative numbers
		// so -0.99999 is a very HIGH weight, and -0.00001 is a very low weight
		if (min_weight_this < min_weight_other) {
			other_ingest.base_reservoir_sample->reservoir_weights.pop();
			other_ingest.base_reservoir_sample->min_weight_threshold =
			    other_ingest.base_reservoir_sample->reservoir_weights.top().first;
		} else {
			base_reservoir_sample->reservoir_weights.pop();
			base_reservoir_sample->min_weight_threshold = base_reservoir_sample->reservoir_weights.top().first;
		}
	}
	D_ASSERT(other_ingest.GetPriorityQueueSize() + GetPriorityQueueSize() <= FIXED_SAMPLE_SIZE);

	// create the two selection vectors that we will use to vector copy from the ingestion samples
	idx_t sample_count_this = GetPriorityQueueSize();
	idx_t sample_count_other = other_ingest.GetPriorityQueueSize();
	D_ASSERT(sample_count_this + sample_count_other == num_samples_to_keep);

	SelectionVector sel_this(sample_count_this);
	SelectionVector sel_other(sample_count_other);

	vector<std::pair<idx_t, double>> this_map;
	vector<std::pair<idx_t, double>> other_map;

	// pop indexes and weights of both samples.
	while (GetPriorityQueueSize() > 0) {
		auto entry = base_reservoir_sample->reservoir_weights.top();
		this_map.push_back(std::make_pair(entry.second, entry.first));
		base_reservoir_sample->reservoir_weights.pop();
	}

	while (other_ingest.GetPriorityQueueSize() > 0) {
		auto entry = other_ingest.base_reservoir_sample->reservoir_weights.top();
		other_map.push_back(std::make_pair(entry.second, entry.first));
		other_ingest.base_reservoir_sample->reservoir_weights.pop();
	}

	D_ASSERT(other_ingest.sample_chunks[0]->GetTypes() == sample_chunks[0]->GetTypes());
	// create new chunk to copy the sample data into
	auto new_chunk = make_uniq<DataChunk>();
	new_chunk->Initialize(Allocator::DefaultAllocator(), sample_chunks[0]->GetTypes(), FIXED_SAMPLE_SIZE);
	for (idx_t col_idx = 0; col_idx < new_chunk->ColumnCount(); col_idx++) {
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(new_chunk->data[col_idx]).Initialize(FIXED_SAMPLE_SIZE);
	}

	auto new_base_reservoir_sampling = make_uniq<BaseReservoirSampling>();

	double max_weight = NumericLimits<double>::Minimum();
	idx_t max_weight_index = 0;
	for (idx_t i = 0; i < this_map.size(); i++) {
		sel_this.set_index(i, this_map[i].first);
		new_base_reservoir_sampling->reservoir_weights.emplace(this_map[i].second, i);
		if (max_weight < this_map[i].second) {
			max_weight = this_map[i].second;
			max_weight_index = i;
		}
	}

	D_ASSERT(sample_count_this <= num_samples_to_keep);
	for (idx_t index = sample_count_this; index < num_samples_to_keep; index++) {
		idx_t index_in_other_map = index - sample_count_this;
		sel_other.set_index(index_in_other_map, other_map[index_in_other_map].first);
		new_base_reservoir_sampling->reservoir_weights.emplace(other_map[index_in_other_map].second, index);
		if (max_weight < other_map[index_in_other_map].second) {
			max_weight = other_map[index_in_other_map].second;
			max_weight_index = index;
		}
	}

	new_base_reservoir_sampling->min_weighted_entry_index = max_weight_index;
	new_base_reservoir_sampling->min_weight_threshold = -max_weight;
	new_base_reservoir_sampling->num_entries_seen_total =
	    base_reservoir_sample->num_entries_seen_total + other_ingest.base_reservoir_sample->num_entries_seen_total;

	for (idx_t col_idx = 0; col_idx < sample_chunks[0]->ColumnCount(); col_idx++) {
		// perform the copy for this
		VectorOperations::Copy(sample_chunks[0]->data[col_idx], new_chunk->data[col_idx], sel_this, sample_count_this,
		                       0, 0);
		// perform copy for other with offset sample_count_this
		VectorOperations::Copy(other_ingest.sample_chunks[0]->data[col_idx], new_chunk->data[col_idx], sel_other,
		                       sample_count_other, 0, sample_count_this);
	}

	new_chunk->SetCardinality(num_samples_to_keep);
	sample_chunks.clear();
	sample_chunks.push_back(std::move(new_chunk));

	base_reservoir_sample = std::move(new_base_reservoir_sampling);
}

idx_t IngestionSample::GetTuplesSeen() {
	return base_reservoir_sample->num_entries_seen_total;
}

unique_ptr<BlockingSample> IngestionSample::ConvertToReservoirSampleToSerialize() {
	Shrink();
	Verify();
	if (sample_chunks.empty() || destroyed) {
		auto ret = make_uniq<ReservoirSample>(FIXED_SAMPLE_SIZE);
		ret->Destroy();
		return ret;
	}

	// since this is for serialization, we really need to make sure keep a
	// minimum of 1% or 2048 values
	idx_t num_samples_to_keep = MinValue<idx_t>(
	    FIXED_SAMPLE_SIZE, static_cast<idx_t>(PERCENTAGE_SAMPLE_SIZE * GetTuplesSeen() / (double(100))));

	vector<std::pair<double, idx_t>> weights_indexes;
	if (base_reservoir_sample->reservoir_weights.empty() && sample_chunks.size() == 1) {
		// we've collected samples but haven't assigned weights yet;
		base_reservoir_sample->InitializeReservoirWeights(sample_chunks[0]->size(), sample_chunks[0]->size());
	}

	auto ret = make_uniq<ReservoirSample>(FIXED_SAMPLE_SIZE);
	ret->base_reservoir_sample = base_reservoir_sample->Copy();

	D_ASSERT(num_samples_to_keep <= ret->GetPriorityQueueSize());
	while (num_samples_to_keep < ret->GetPriorityQueueSize()) {
		ret->PopFromWeightQueue();
	}
	D_ASSERT(num_samples_to_keep == ret->GetPriorityQueueSize());

	for (idx_t i = 0; i < num_samples_to_keep; i++) {
		weights_indexes.push_back(ret->PopFromWeightQueue());
	}

	// ingestion sample has already been shrunk so there is only one sample chunk
	D_ASSERT(sample_chunks.size() == 1);
	auto &chunk_to_copy = sample_chunks[0];

	// create a new sample chunk to store new samples
	ret->reservoir_chunk = make_uniq<ReservoirChunk>();
	ret->reservoir_chunk->chunk.Initialize(Allocator::DefaultAllocator(), chunk_to_copy->GetTypes(), FIXED_SAMPLE_SIZE);
	for (idx_t col_idx = 0; col_idx < ret->reservoir_chunk->chunk.ColumnCount(); col_idx++) {
		// TODO: should the validity mask be the capacity or the size?
		FlatVector::Validity(ret->reservoir_chunk->chunk.data[col_idx]).Initialize(FIXED_SAMPLE_SIZE);
	}
	ret->reservoir_chunk->chunk.SetCardinality(num_samples_to_keep);

	// set up selection vector to copy IngestionSample to ReservoirSample
	SelectionVector sel(num_samples_to_keep);
	// make sure the reservoir weights are empty. We will reconstruct the heap with new indexes
	// and the same weights
	D_ASSERT(ret->GetPriorityQueueSize() == 0);
	double max_weight = NumericLimits<double>::Minimum();
	idx_t max_weight_index = 0;
	for (idx_t i = 0; i < num_samples_to_keep; i++) {
		sel.set_index(i, weights_indexes[i].second);
		ret->base_reservoir_sample->reservoir_weights.emplace(weights_indexes[i].first, i);
		if (max_weight < weights_indexes[i].first) {
			max_weight = weights_indexes[i].first;
			max_weight_index = i;
		}
	}
	ret->base_reservoir_sample->min_weighted_entry_index = max_weight_index;
	ret->base_reservoir_sample->min_weight_threshold = -max_weight;

	// perform the copy
	for (idx_t col_idx = 0; col_idx < chunk_to_copy->ColumnCount(); col_idx++) {
		VectorOperations::Copy(chunk_to_copy->data[col_idx], ret->reservoir_chunk->chunk.data[col_idx], sel,
		                       num_samples_to_keep, 0, 0);
	}
	D_ASSERT(ret->GetPriorityQueueSize() == ret->reservoir_chunk->chunk.size());
	return ret;
}

idx_t IngestionSample::CreateFirstChunk(DataChunk &chunk) {
	unique_ptr<DataChunk> new_sample_chunk;
	idx_t offset = 0;
	idx_t source_count = chunk.size();
	idx_t required_count = FIXED_SAMPLE_SIZE;
	idx_t first_chunk_cardinality = chunk.size();
	if (sample_chunks.empty()) {
		if (chunk.size() > FIXED_SAMPLE_SIZE) {
			throw InternalException("Creating sample with DataChunk that is larger than the fixed sample size");
		}
		// create a new sample chunk to store new samples
		new_sample_chunk = make_uniq<DataChunk>();
		new_sample_chunk->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes(), FIXED_SAMPLE_SIZE);
		for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
			FlatVector::Validity(new_sample_chunk->data[col_idx]).Initialize(FIXED_SAMPLE_SIZE);
		}
		required_count = chunk.size();
	} else {
		D_ASSERT(sample_chunks.size() == 1);
		new_sample_chunk = std::move(sample_chunks[0]);
		required_count = MinValue(FIXED_SAMPLE_SIZE - new_sample_chunk->size(), chunk.size());
		if (chunk.size() > required_count) {
			source_count = required_count;
			D_ASSERT(new_sample_chunk->size() + source_count == FIXED_SAMPLE_SIZE);
		}
		offset = new_sample_chunk->size();
		sample_chunks.clear();
		first_chunk_cardinality = new_sample_chunk->size() + required_count;
	}
	D_ASSERT(new_sample_chunk->ColumnCount() == chunk.ColumnCount());
	for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
		VectorOperations::Copy(chunk.data[col_idx], new_sample_chunk->data[col_idx], required_count, 0, offset);
	}
	new_sample_chunk->SetCardinality(first_chunk_cardinality);
	sample_chunks.push_back(std::move(new_sample_chunk));

	if (chunk.size() - required_count) {
		return chunk.size();
	}
	return required_count;
}

IngestionSample::IngestionSample(idx_t sample_count, int64_t seed)
    : BlockingSample(seed), sample_count(sample_count), allocator(Allocator::DefaultAllocator()), destroyed(false) {
	base_reservoir_sample = make_uniq<BaseReservoirSampling>(seed);
	type = SampleType::INGESTION_SAMPLE;
}

IngestionSample::IngestionSample(Allocator &allocator, int64_t seed)
    : BlockingSample(seed), sample_count(FIXED_SAMPLE_SIZE), allocator(allocator), destroyed(false) {
	base_reservoir_sample = make_uniq<BaseReservoirSampling>(seed);
	type = SampleType::INGESTION_SAMPLE;
}

void IngestionSample::Destroy() {
	destroyed = true;
}

idx_t IngestionSample::GetReplacementCount(idx_t theoretical_chunk_length) {
	auto base_reservoir_sample_copy = base_reservoir_sample->Copy();
	idx_t remaining = theoretical_chunk_length;
	idx_t ret = 0;

	while (true) {
		idx_t offset = base_reservoir_sample_copy->next_index_to_sample -
		               base_reservoir_sample_copy->num_entries_to_skip_b4_next_sample;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			return ret;
		}
		// in this chunk! replace the element
		ret += 1;
		base_reservoir_sample_copy->ReplaceElement();
		// shift the chunk forward
		remaining -= offset;
	}
}

void IngestionSample::Finalize() {
	return;
}

bool IngestionSample::SampleWasCut() {
	return sample_chunks.size() == 1 && !base_reservoir_sample->reservoir_weights.empty() &&
	       sample_chunks.at(0)->size() < FIXED_SAMPLE_SIZE;
}

void IngestionSample::AddToReservoir(DataChunk &chunk) {
	if (destroyed) {
		return;
	}
	if (sample_chunks.empty() || NumSamplesCollected() < FIXED_SAMPLE_SIZE) {

		idx_t tuples_consumed = CreateFirstChunk(chunk);
		base_reservoir_sample->num_entries_seen_total += tuples_consumed;
		D_ASSERT(sample_chunks.size() == 1);

		// if there are reservoir weights, and we've consumed tuples, then we need to give
		// them weights
		if (GetPriorityQueueSize() > 0 && tuples_consumed > 0) {
			base_reservoir_sample->InitializeReservoirWeights(tuples_consumed, tuples_consumed, GetPriorityQueueSize());
		}
		Verify();
		// the chunk filled the first FIXED_SAMPLE_SIZE chunk but still has tuples remaining
		// slice the chunk and call AddToReservoir again.
		if (tuples_consumed != chunk.size()) {
			// means this chunk fills the first sample chunk and then some.
			// So we slice it and add it to the ingestion sample.
			auto slice = make_uniq<DataChunk>();
			auto samples_remaining = chunk.size() - tuples_consumed;
			auto types = chunk.GetTypes();
			SelectionVector sel(samples_remaining);
			for (idx_t i = 0; i < samples_remaining; i++) {
				sel.set_index(i, tuples_consumed + i);
			}
			slice->Initialize(Allocator::DefaultAllocator(), types.begin(), types.end(), samples_remaining);
			slice->Slice(chunk, sel, samples_remaining);
			slice->SetCardinality(samples_remaining);
			AddToReservoir(*slice);
		}
		Verify();
		return;
	}

	base_reservoir_sample->num_entries_seen_total += chunk.size();

	// make sure we have sampling weights
	if (GetPriorityQueueSize() != FIXED_SAMPLE_SIZE) {
		D_ASSERT(sample_chunks[0]->size() == FIXED_SAMPLE_SIZE);
		auto num_weights_assigned = GetPriorityQueueSize();
		auto remaining = FIXED_SAMPLE_SIZE - num_weights_assigned;
		base_reservoir_sample->InitializeReservoirWeights(remaining, remaining, num_weights_assigned);
	}

	// run the logic to figure out which indexes in the sample will get booted.
	idx_t remaining = chunk.size();
	idx_t base_offset = 0;
	vector<idx_t> indexes_to_copy;
	while (true) {
		idx_t offset =
		    base_reservoir_sample->next_index_to_sample - base_reservoir_sample->num_entries_to_skip_b4_next_sample;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample->num_entries_to_skip_b4_next_sample += remaining;
			break;
		}
		// in this chunk! replace the element
		indexes_to_copy.push_back(base_offset + offset);
		base_reservoir_sample->ReplaceElement();
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}

	if (indexes_to_copy.size() == 0) {
		Verify();
		// we don't need to sample anymore
		return;
	}

	// create a new sample chunk to store new samples
	auto new_sample_chunk = make_uniq<DataChunk>();
	new_sample_chunk->Initialize(Allocator::DefaultAllocator(), chunk.GetTypes(), indexes_to_copy.size());
	for (idx_t col_idx = 0; col_idx < new_sample_chunk->ColumnCount(); col_idx++) {
		FlatVector::Validity(new_sample_chunk->data[col_idx]).Initialize(indexes_to_copy.size());
	}

	new_sample_chunk->SetCardinality(indexes_to_copy.size());
	SelectionVector sel(indexes_to_copy.size());
	for (idx_t i = 0; i < indexes_to_copy.size(); i++) {
		sel.set_index(i, indexes_to_copy[i]);
	}
	const SelectionVector const_sel(sel);

	for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
		VectorOperations::Copy(chunk.data[col_idx], new_sample_chunk->data[col_idx], const_sel, indexes_to_copy.size(),
		                       0, 0);
	}

	// using vector operations copy into it only the desired values
	idx_t offset_in_ingestion_sample = 0;
	for (auto &sample_chunk : sample_chunks) {
		offset_in_ingestion_sample += sample_chunk->size();
	}
	idx_t new_index = offset_in_ingestion_sample;
	// I actually don't care about what indexes I'm copying. I've already copied data from the
	// source/ingested chunk to my new sample chunk. I need to record the indexes of the new samples
	// in the sampling info
	for (auto &copied_index : indexes_to_copy) {
		auto sample_to_replace = base_reservoir_sample->reservoir_weights.top();
		base_reservoir_sample->reservoir_weights.pop();
		auto new_weight = -sample_to_replace.first;
		// careful here, only replace the element in sampling info.
		base_reservoir_sample->ReplaceElementWithIndex(new_index, new_weight);
		new_index += 1;
	}
	D_ASSERT(base_reservoir_sample->reservoir_weights.size() == FIXED_SAMPLE_SIZE);
	Verify();
	sample_chunks.push_back(std::move(new_sample_chunk));
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
	D_ASSERT(type == SampleType::RESERVOIR_SAMPLE);
	result = ReservoirSample::Deserialize(deserializer);
	result->base_reservoir_sample = std::move(base_reservoir_sample);
	result->destroyed = destroyed;
	return result;
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
	base_reservoir_sample->reservoir_weights.emplace(
	    std::make_pair(NumericLimits<double>::Maximum(), idx_t(copy_percentage.sample_percentage * 100)));
}

unique_ptr<BlockingSample> ReservoirSamplePercentage::Deserialize(Deserializer &deserializer) {
	auto sample_percentage = deserializer.ReadProperty<double>(200, "sample_percentage");
	auto result = duckdb::unique_ptr<ReservoirSamplePercentage>(new ReservoirSamplePercentage(sample_percentage));
	deserializer.ReadPropertyWithDefault<idx_t>(201, "reservoir_sample_size", result->reservoir_sample_size);
	return std::move(result);
}

} // namespace duckdb
