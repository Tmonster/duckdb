#include "duckdb/execution/operator/helper/physical_reservoir_sample.hpp"
#include "duckdb/execution/reservoir_sample.hpp"
#include "iostream"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SampleLocalSinkState : public LocalSinkState {
public:
	explicit SampleLocalSinkState(ClientContext &context, const PhysicalReservoirSample &sampler, SampleOptions &options) {
		// Here I need to initialize the reservoir sample again from the local state.
		auto &allocator = Allocator::Get(context);
		if (options.is_percentage) {
			auto percentage = options.sample_size.GetValue<double>();
			if (percentage == 0) {
				return;
			}
			sample = make_unique<ReservoirSamplePercentage>(allocator, percentage, options.seed);
		} else {
			auto size = options.sample_size.GetValue<int64_t>();
			if (size == 0) {
				return;
			}
			sample = make_unique<ReservoirSample>(allocator, size, options.seed);
		}
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
};

class SampleGlobalSinkState : public GlobalSinkState {
public:
	explicit SampleGlobalSinkState(Allocator &allocator, SampleOptions &options) {
		if (options.is_percentage) {
			auto percentage = options.sample_size.GetValue<double>();
			if (percentage == 0) {
				return;
			}
			sample = make_unique<ReservoirSamplePercentage>(allocator, percentage, options.seed);
		} else {
			auto size = options.sample_size.GetValue<int64_t>();
			if (size == 0) {
				return;
			}
			sample = make_unique<ReservoirSample>(allocator, size, options.seed);
		}
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
	vector<intermediate_sample_and_pop_count> intermediate_samples;
};

unique_ptr<GlobalSinkState> PhysicalReservoirSample::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<SampleGlobalSinkState>(Allocator::Get(context), *options);
}

unique_ptr<LocalSinkState> PhysicalReservoirSample::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<SampleLocalSinkState>(context.client, *this, *options);
}

SinkResultType PhysicalReservoirSample::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                             DataChunk &input) const {
	auto &local_state = (SampleLocalSinkState &)lstate;
	if (!local_state.sample) {
		return SinkResultType::FINISHED;
	}
	// here we add samples to local state, then we eventually combine them in the global state using Combine()
	// Why am I confused?
	// When do I know when a thread collects no more data?
	// Is there a way to know how much data a thread will eventually collect?

	// we implement reservoir sampling without replacement and exponential jumps here
	// the algorithm is adopted from the paper Weighted random sampling with a reservoir by Pavlos S. Efraimidis et al.
	// note that the original algorithm is about weighted sampling; this is a simplified approach for uniform sampling
	local_state.sample->AddToReservoir(input);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalReservoirSample::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
	auto &global_state = (SampleGlobalSinkState &)gstate;
	auto &local_state = (SampleLocalSinkState &)lstate;
	lock_guard<mutex> glock(global_state.lock);
	global_state.intermediate_samples.push_back(intermediate_sample_and_pop_count(move(local_state.sample), 0));
}

SinkFinalizeType PhysicalReservoirSample::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, GlobalSinkState &gstate) const {
	auto &global_state = (SampleGlobalSinkState &)gstate;
	auto total_count = 0;
	for (auto &sample : global_state.intermediate_samples) {
		total_count += sample.isample->base_reservoir_sample.num_entries_seen_total;
	}
	idx_t sample_count;
	if (options->is_percentage) {
		auto percentage = options->sample_size.GetValue<double>();
		sample_count = percentage * total_count;
	} else {
		auto size = options->sample_size.GetValue<int64_t>();
		sample_count = size;
	}

	auto weights = vector<double>();
	for (idx_t i = 0; i < global_state.intermediate_samples.size(); i++) {
		// get the proper amount of data for the sample.
		// calculate sample_to_add, num_entries_seen_total / total_count
		// Call sample->GetChunk until you get samples_to_add.
		auto &sample = global_state.intermediate_samples.at(i);
		double fraction_of_samples_to_add = sample.isample->base_reservoir_sample.num_entries_seen_total / total_count;
		sample.weight = fraction_of_samples_to_add;
	}
	for (idx_t j = 0; j < sample_count; j++) {
		// generate random number in between 0 and sample_count
		// check which sample should be popped from and increase the sample pop count
	}
	for (idx_t k = 0; k < global_state.intermediate_samples.size(); k++) {
		// add chunks to the global sample until the pop count for the sample reaches 0
	}

	global_state.intermediate_samples.clear();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
void PhysicalReservoirSample::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	auto &sink = (SampleGlobalSinkState &)*this->sink_state;
	if (!sink.sample) {
		return;
	}
	auto sample_chunk = sink.sample->GetChunk();
	if (!sample_chunk) {
		return;
	}
	chunk.Move(*sample_chunk);
}

string PhysicalReservoirSample::ParamsToString() const {
	return options->sample_size.ToString() + (options->is_percentage ? "%" : " rows");
}

} // namespace duckdb
