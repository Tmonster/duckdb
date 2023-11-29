#include "duckdb/execution/operator/helper/physical_reservoir_sample.hpp"
#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/atomic.hpp"
#include "iostream"


namespace duckdb {

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class SampleLocalSinkState : public LocalSinkState {
public:
	explicit SampleLocalSinkState(ClientContext &context, const PhysicalReservoirSample &sampler, SampleOptions &options) {
		// Here I need to initialize the reservoir sample again from the local state.
		// samples get initialized during first sink of the thread.
	    sample = nullptr;
	}
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
};

class SampleGlobalSinkState : public GlobalSinkState {
public:
	explicit SampleGlobalSinkState(Allocator &allocator, SampleOptions &options) {
		threads = 1;
		if (options.is_percentage) {
			auto percentage = options.sample_size.GetValue<double>();
			if (percentage == 0) {
				return;
			}
			sample = make_uniq<ReservoirSamplePercentage>(allocator, percentage, options.seed);
		} else {
			auto size = options.sample_size.GetValue<int64_t>();
			if (size == 0) {
				return;
			}
			sample = make_uniq<ReservoirSample>(allocator, size, options.seed);
		}
	}

	template<typename T>
	T GetSampleCountAndIncreaseThreads(T size_or_percentage) {
		lock_guard<mutex> glock(lock);
		auto thread_size = size_or_percentage;
		for (idx_t i = 1; i < threads; i++) {
			thread_size = thread_size / 2;
		}
		threads += 1;
		return thread_size;
	}

	//! The lock for updating the global aggoregate state
	mutex lock;
	atomic<idx_t> threads;
	//! The reservoir sample
	unique_ptr<BlockingSample> sample;
	vector<unique_ptr<BlockingSample>> intermediate_samples;
};

unique_ptr<GlobalSinkState> PhysicalReservoirSample::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<SampleGlobalSinkState>(Allocator::Get(context), *options);
}

unique_ptr<LocalSinkState> PhysicalReservoirSample::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<SampleLocalSinkState>(context.client, *this, *options);
}

SinkResultType PhysicalReservoirSample::Sink(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<SampleGlobalSinkState>();
	auto &local_state = input.local_state.Cast<SampleLocalSinkState>();

	// if there is no local state sample, create one and increase the thread count
	// the size of a threads sample is dependent on how many threads are already collecting samples
	// This helps us collect only O(2n) size samples instead of O(p*n) where p = # of processes
	if (!local_state.sample) {
		auto &allocator = Allocator::Get(context.client);
		if (options->is_percentage) {
			// always gather full thread percentage
			double thread_percentage = options->sample_size.GetValue<double>();
			// This is a magic number.
			if (thread_percentage == 0) {
				return SinkResultType::FINISHED;
			}
			local_state.sample = make_uniq<ReservoirSamplePercentage>(allocator, thread_percentage, options->seed);
		} else {
			idx_t thread_size = global_state.GetSampleCountAndIncreaseThreads(options->sample_size.GetValue<idx_t>());
			if (thread_size == 0) {
				return SinkResultType::FINISHED;
			}
			local_state.sample = make_uniq<ReservoirSample>(allocator, thread_size, options->seed);
		}
	}
	D_ASSERT(local_state.sample);
	// we implement reservoir sampling without replacement and exponential jumps here
	// the algorithm is adopted from the paper Weighted random sampling with a reservoir by Pavlos S. Efraimidis et al.
	// note that the original algorithm is about weighted sampling; this is a simplified approach for uniform sampling
	lock_guard<mutex> glock(global_state.lock);
	global_state.sample->AddToReservoir(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalReservoirSample::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<SampleGlobalSinkState>();
	auto &local_state = input.local_state.Cast<SampleLocalSinkState>();
	lock_guard<mutex> glock(global_state.lock);
	// for some reason here local_state.sample doesn't have the info anymore.
	if (local_state.sample) {
		// check if the local state sample even did sampling.
	}
	global_state.intermediate_samples.push_back(std::move(global_state.sample));
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalReservoirSample::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<SampleGlobalSinkState>();
	D_ASSERT(global_state.intermediate_samples.size() >= 1);
	auto last_sample = std::move(global_state.intermediate_samples.back());
	global_state.intermediate_samples.pop_back();
	for (auto &sample : global_state.intermediate_samples) {
		last_sample->Merge(sample);
	}
	last_sample->Finalize();
	global_state.sample = std::move(last_sample);
	global_state.intermediate_samples.clear();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalReservoirSample::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &sink = this->sink_state->Cast<SampleGlobalSinkState>();
	if (!sink.sample) {
		return SourceResultType::FINISHED;
	}
	auto sample_chunk = sink.sample->GetChunk();
	if (!sample_chunk) {
		return SourceResultType::FINISHED;
	}
	chunk.Move(*sample_chunk);

	return SourceResultType::HAVE_MORE_OUTPUT;
}

string PhysicalReservoirSample::ParamsToString() const {
	return options->sample_size.ToString() + (options->is_percentage ? "%" : " rows");
}

} // namespace duckdb
