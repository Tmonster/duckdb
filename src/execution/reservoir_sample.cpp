#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/pair.hpp"
#include "iostream"

namespace duckdb {

ReservoirSample::ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed)
    : BlockingSample(seed), allocator(allocator), num_added_samples(0), sample_count(sample_count),
      reservoir_initialized(false) {
}

void ReservoirSample::AddToReservoir(DataChunk &input) {
	if (sample_count == 0) {
		return;
	}
	base_reservoir_sample.num_entries_seen_total += input.size();
	// Input: A population V of n weighted items
	// Output: A reservoir R with a size m
	// 1: The first m items of V are inserted into R
	// first we need to check if the reservoir already has "m" elements
	if (num_added_samples < sample_count) {
		if (FillReservoir(input) == 0) {
			// entire chunk was consumed by reservoir
			return;
		}
	}
	D_ASSERT(reservoir_chunk->GetCapacity() == sample_count);
	// find the position of next_index_to_sample relative to number of seen entries (num_entries_to_skip_b4_next_sample)
	idx_t remaining = input.size();
	idx_t base_offset = 0;
	while (true) {
		idx_t offset = base_reservoir_sample.next_index_to_sample - base_reservoir_sample.num_entries_to_skip_b4_next_sample;
		if (offset >= remaining) {
			// not in this chunk! increment current count and go to the next chunk
			base_reservoir_sample.num_entries_to_skip_b4_next_sample += remaining;
			return;
		}
		D_ASSERT(reservoir_chunk->GetCapacity() == sample_count);
		// in this chunk! replace the element
		ReplaceElement(input, base_offset + offset);
		// shift the chunk forward
		remaining -= offset;
		base_offset += offset;
	}
}

void ReservoirSample::Merge(unique_ptr<BlockingSample> &other) {
	// 1. First pop pairs from other.base_reservoir_sample.reservoir_weights until
	//    you have an element with a weight above the
	auto &other_as_rs = (ReservoirSample&)*other;
	auto min_weight_other = other->base_reservoir_sample.reservoir_weights.top();
	// need to remember indexes of samples you want to replace
	vector<std::pair<double, idx_t>> temporary_queue;
	idx_t samples_to_replace = 0;
	while (samples_to_replace < other->base_reservoir_sample.reservoir_weights.size()) {
		while ((-1 * min_weight_other.first) < base_reservoir_sample.min_weight_threshold) {
			other->base_reservoir_sample.reservoir_weights.pop();
			min_weight_other = other->base_reservoir_sample.reservoir_weights.top();
		}
		samples_to_replace += 1;
		temporary_queue.push_back(base_reservoir_sample.reservoir_weights.top());
		base_reservoir_sample.reservoir_weights.pop();
		base_reservoir_sample.min_weight_threshold = base_reservoir_sample.reservoir_weights.top().first * -1;
		other->base_reservoir_sample.reservoir_weights.pop();
		min_weight_other = other->base_reservoir_sample.reservoir_weights.top();
	}


	// 2. If all weights are less than this.min_weight_threshold, no merge needs to take place
	if (other->base_reservoir_sample.reservoir_weights.size() == 0) {
		return;
	}

	idx_t replaced_element_count = 0;
	// 3. All entries in other can now go into this.reservoir sample
	for (auto &weight_pair : temporary_queue) {
		min_weight_other = other->base_reservoir_sample.reservoir_weights.top();
		other->base_reservoir_sample.reservoir_weights.pop();
		base_reservoir_sample.min_weighted_entry_index = weight_pair.second;
		// replace element in reservoir chunk with the weight from the other reservoir chunk
		ReplaceElement(*other_as_rs.reservoir_chunk, min_weight_other.second, min_weight_other.first);
		replaced_element_count++;
	}
}

unique_ptr<DataChunk> ReservoirSample::GetChunk() {
	if (num_added_samples == 0) {
		return nullptr;
	}
	D_ASSERT(reservoir_chunk->GetCapacity() == sample_count);
	if (reservoir_chunk->size() > STANDARD_VECTOR_SIZE) {
		// get from the back
		auto ret = make_uniq<DataChunk>();
		auto samples_remaining = num_added_samples - STANDARD_VECTOR_SIZE;
		auto reservoir_types = reservoir_chunk->GetTypes();
		SelectionVector sel(STANDARD_VECTOR_SIZE);
		for (idx_t i = samples_remaining; i < num_added_samples; i++) {
			sel.set_index(i - samples_remaining, i);
		}
		ret->Initialize(allocator, reservoir_types.begin(), reservoir_types.end(), STANDARD_VECTOR_SIZE);
		reservoir_chunk->Slice(*ret, sel, STANDARD_VECTOR_SIZE);
		ret->SetCardinality(STANDARD_VECTOR_SIZE);
		// reduce capacity and cardinality of the sample data chunk
		reservoir_chunk->SetCardinality(samples_remaining);
		num_added_samples = samples_remaining;
		return ret;
	}
	// TODO: Why do I need to put another selection vector over this one?
	auto ret = make_uniq<DataChunk>();
	auto samples_remaining = 0;
	auto reservoir_types = reservoir_chunk->GetTypes();
	SelectionVector sel(num_added_samples);
	for (idx_t i = 0; i < num_added_samples; i++) {
		sel.set_index(i, i);
	}
	ret->Initialize(allocator, reservoir_types.begin(), reservoir_types.end(), num_added_samples);
	reservoir_chunk->Slice(*ret, sel, num_added_samples);
	ret->SetCardinality(num_added_samples);
	// reduce capacity and cardinality of the sample data chunk
	reservoir_chunk->SetCardinality(samples_remaining);
	num_added_samples = 0;
	return ret;
//	num_added_samples = 0;
//	return std::move(reservoir_chunk);
}

void ReservoirSample::ReplaceElement(DataChunk &input, idx_t index_in_chunk, double with_weight) {
	// replace the entry in the reservoir
	// 8. The item in R with the minimum key is replaced by item
	D_ASSERT(input.ColumnCount() == reservoir_chunk->ColumnCount());
	D_ASSERT(reservoir_chunk->GetCapacity() == sample_count);
	for (idx_t col_idx = 0; col_idx < input.ColumnCount(); col_idx++) {
		D_ASSERT(reservoir_chunk->GetCapacity() == sample_count);
		reservoir_chunk->SetValue(col_idx, base_reservoir_sample.min_weighted_entry_index,
		                          input.GetValue(col_idx, index_in_chunk));
	}
	base_reservoir_sample.ReplaceElement(with_weight);
}

void ReservoirSample::Finalize() {
	return;
}

void ReservoirSample::InitializeReservoir(DataChunk &input) {
	reservoir_chunk = make_uniq<DataChunk>();
	reservoir_chunk->Initialize(allocator, input.GetTypes(), sample_count);
	for (idx_t col_idx = 0; col_idx < reservoir_chunk->ColumnCount(); col_idx++) {
		FlatVector::Validity(reservoir_chunk->data[col_idx]).Initialize(sample_count);
	}
	reservoir_initialized = true;
}

idx_t ReservoirSample::FillReservoir(DataChunk &input) {
	idx_t chunk_count = input.size();
	input.Flatten();
	D_ASSERT(num_added_samples <= sample_count);

	// we have not: append to the reservoir
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
	if (!reservoir_initialized) {
		InitializeReservoir(input);
	}
	reservoir_chunk->Append(input, false, nullptr, required_count);
	base_reservoir_sample.InitializeReservoir(reservoir_chunk->size(), sample_count);

	num_added_samples += required_count;

	D_ASSERT(reservoir_chunk->GetCapacity() == sample_count);
	reservoir_chunk->SetCardinality(num_added_samples);

	// check if there are still elements remaining in the Input data chunk that should be
	// randomly sampled and potentially added. This happens if we are on a boundary
	// for example, input.size() is 1024, but our sample size is 10
	if (required_count == chunk_count) {
		// we are done here
		return 0;
	}
	// we still need to process a part of the chunk
	// create a selection vector of the remaining elements
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	for (idx_t i = required_count; i < chunk_count; i++) {
		sel.set_index(i - required_count, i);
	}
	// slice the input vector and continue
	input.Slice(sel, chunk_count - required_count);
	D_ASSERT(reservoir_chunk->GetCapacity() == sample_count);
	return input.size();
}

ReservoirSamplePercentage::ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed)
    : BlockingSample(seed), allocator(allocator), sample_percentage(percentage / 100.0), current_count(0),
      is_finalized(false) {
	reservoir_sample_size = idx_t(sample_percentage * RESERVOIR_THRESHOLD);
	current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
}

void ReservoirSamplePercentage::Merge(unique_ptr<BlockingSample> &other) {
	//! We are now merging all the samples. 80% of every sample should equal 80%
	//! of all rows so we set sample percentage to 1, which will means every tuple
	//! in the added chunks will be added
	sample_percentage = 1;
	auto chunk = other->GetChunk();
	while (chunk) {
		AddToReservoir(*chunk);
		chunk = other->GetChunk();
	}
}

void ReservoirSamplePercentage::AddToReservoir(DataChunk &input) {
	base_reservoir_sample.num_entries_seen_total += input.size();
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
			for (idx_t i = 0; i < append_to_next_sample; i++) {
				sel.set_index(i, append_to_current_sample_count + i);
			}
			input.Slice(sel, append_to_next_sample);
		}
		// now our first sample is filled: append it to the set of finished samples
		finished_samples.push_back(std::move(current_sample));

		// allocate a new sample, and potentially add the remainder of the current input to that sample
		current_sample = make_uniq<ReservoirSample>(allocator, reservoir_sample_size, random.NextRandomInteger());
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

unique_ptr<DataChunk> ReservoirSamplePercentage::GetChunk() {
	if (!is_finalized) {
		Finalize();
	}
	while (!finished_samples.empty()) {
		auto &front = finished_samples.front();
		auto chunk = front->GetChunk();
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
	    current_count < sample_percentage * RESERVOIR_THRESHOLD || finished_samples.empty();
	if (current_count > 0 && sampled_more_than_required) {
		// create a new sample
		auto new_sample_size = idx_t(round(sample_percentage * current_count));
		auto new_sample = make_uniq<ReservoirSample>(allocator, new_sample_size, random.NextRandomInteger());
		while (true) {
			auto chunk = current_sample->GetChunk();
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

BaseReservoirSampling::BaseReservoirSampling(int64_t seed) : random(seed) {
	next_index_to_sample = 0;
	min_weight_threshold = 0;
	min_weighted_entry_index = 0;
	num_entries_to_skip_b4_next_sample = 0;
	num_entries_seen_total = 0;
}

BaseReservoirSampling::BaseReservoirSampling() : BaseReservoirSampling(-1) {
}

void BaseReservoirSampling::InitializeReservoir(idx_t cur_size, idx_t sample_size) {
	//! 1: The first m items of V are inserted into R
	//! first we need to check if the reservoir already has "m" elements
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
	if (cur_size > sample_size) {
		throw InternalException("cur_size should eventually be equal to sample size right?");
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
	reservoir_weights.push(std::make_pair(-r2, min_weighted_entry_index));
	//! we update the min entry with the new min entry in the reservoir
	SetNextEntry();
}

} // namespace duckdb
