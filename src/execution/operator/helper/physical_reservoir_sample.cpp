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

	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
	vector<unique_ptr<BlockingSample>> intermediate_samples;
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
	global_state.intermediate_samples.push_back(move(local_state.sample));
}

SinkFinalizeType PhysicalReservoirSample::Finalize(Pipeline &pipeline, Event &event, ClientContext &context, GlobalSinkState &gstate) const {
	auto &global_state = (SampleGlobalSinkState &)gstate;
	global_state.sample = move(global_state.intermediate_samples.back());
	global_state.intermediate_samples.pop_back();
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
