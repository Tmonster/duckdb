//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/reservoir_sample.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include "duckdb/common/queue.hpp"

namespace duckdb {

enum class SampleType : uint8_t { BLOCKING_SAMPLE = 0, RESERVOIR_SAMPLE = 1, RESERVOIR_PERCENTAGE_SAMPLE = 2 };

//! Resevoir sampling is based on the 2005 paper "Weighted Random Sampling" by Efraimidis and Spirakis
class BaseReservoirSampling {
public:
	explicit BaseReservoirSampling(int64_t seed);
	BaseReservoirSampling();

	void InitializeReservoirWeights(idx_t cur_size, idx_t sample_size);

	void SetNextEntry();

	void ReplaceElementWithIndex(idx_t entry_index, double with_weight);
	void ReplaceElement(double with_weight = -1);

	void IncreaseNumEntriesSeenTotal(idx_t count);

	unique_ptr<BaseReservoirSampling> Copy();
	//! The random generator
	RandomEngine random;

	//! The next element to sample
	idx_t next_index_to_sample;
	//! The reservoir threshold of the current min entry
	double min_weight_threshold;
	//! The reservoir index of the current min entry
	idx_t min_weighted_entry_index;
	//! The current count towards next index (i.e. we will replace an entry in next_index - current_count tuples)
	//! The number of entries "seen" before choosing one that will go in our reservoir sample.
	idx_t num_entries_to_skip_b4_next_sample;
	//! when collecting a sample in parallel, we want to know how many values each thread has seen
	//! so we can collect the samples from the thread local states in a uniform manner
	idx_t num_entries_seen_total;
	//! Priority queue of [random element, index] for each of the elements in the sample
	std::priority_queue<std::pair<double, idx_t>> reservoir_weights;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<BaseReservoirSampling> Deserialize(Deserializer &deserializer);

	// Blocking sample is a virtual class. It should be allowed to see the weights and
	// of tuples in the sample. The blocking sample can then easily maintain statisitcal properties
	// from the sample point of view.
	friend class BlockingSample;
};

class BlockingSample {
public:
	static constexpr const SampleType TYPE = SampleType::BLOCKING_SAMPLE;

	unique_ptr<BaseReservoirSampling> base_reservoir_sample;
	//! The sample type
	SampleType type;
	//! has the sample been destroyed due to updates to the referenced table
	bool destroyed;

public:
	explicit BlockingSample(int64_t seed = -1)
	    : base_reservoir_sample(make_uniq<BaseReservoirSampling>(seed)), type(SampleType::BLOCKING_SAMPLE),
	      destroyed(false) {
	}
	virtual ~BlockingSample() {
	}

	virtual idx_t NumSamplesCollected() = 0;

	//! Add a chunk of data to the sample
	virtual void AddToReservoir(DataChunk &input) = 0;

	virtual void Merge(unique_ptr<BlockingSample> other) = 0;

	virtual unique_ptr<BlockingSample> Copy() const = 0;

	virtual void Finalize() = 0;

	//! Used to convert regular samples to reservoir percentage samples.
	//! This is used during deserialization. Percentage reservoir samples
	//! are serialized as reservoir samples when a table has less than 204800 rows
	//! When deserialization happens, we want to deserialize back to a percentage sample.
	static unique_ptr<BlockingSample> MaybeConvertReservoirToPercentageResevoir(unique_ptr<BlockingSample> sample);

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used when
	//! querying from a live sample and not a table collected sample.
	virtual unique_ptr<DataChunk> GetChunkAndShrink() = 0;
	virtual unique_ptr<DataChunk> GetChunk(idx_t offset = 0) = 0;
	virtual void Destroy();

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

protected:
	//! Helper functions needed to merge two reservoirs while respecting weights of sampled rows
	std::pair<double, idx_t> PopFromWeightQueue();
	double GetMinWeightThreshold();
	idx_t GetPriorityQueueSize();

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE && TARGET::TYPE != SampleType::BLOCKING_SAMPLE) {
			throw InternalException("Failed to cast sample to type - sample type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE && TARGET::TYPE != SampleType::BLOCKING_SAMPLE) {
			throw InternalException("Failed to cast sample to type - sample type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class ReservoirChunk {
public:
	ReservoirChunk() {
	}

	DataChunk chunk;
	void Serialize(Serializer &serializer) const;
	static unique_ptr<ReservoirChunk> Deserialize(Deserializer &deserializer);

	unique_ptr<ReservoirChunk> Copy() const;
};

//! The reservoir sample class maintains a streaming sample of fixed size "sample_count"
class ReservoirSample : public BlockingSample {
public:
	static constexpr const SampleType TYPE = SampleType::RESERVOIR_SAMPLE;

public:
	ReservoirSample(Allocator &allocator, idx_t sample_count, int64_t seed = 1);
	explicit ReservoirSample(idx_t sample_count, int64_t seed = 1);

	idx_t NumSamplesCollected() override;
	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	void Merge(unique_ptr<BlockingSample> other) override;

	//! Special Merge.
	//! Here you have multiple reservoir samples (small_samples) with smaple counts smaller than this.sample_count
	//! and you want one large reservoir sample while only maintaining the highest this.sample_count weights
	//! This is used when converting a ReservoirPercentageSample into a ReservoirSample
	//! and the ReservoirPercentageSample has more samples than this.sample_count.
	void CombineMerge(vector<unique_ptr<ReservoirSample>> small_samples);

	unique_ptr<BlockingSample> Copy() const override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunkAndShrink() override;
	unique_ptr<DataChunk> GetChunk(idx_t offset = 0) override;
	void Destroy() override;
	void Finalize() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

private:
	//! Replace a single element of the input
	void ReplaceElement(DataChunk &input, idx_t index_in_chunk, double with_weight = -1);
	void ReplaceElement(idx_t reservoir_chunk_index, DataChunk &input, idx_t index_in_input_chunk, double with_weight);

	void CreateReservoirChunk(const vector<LogicalType> &types);
	//! Fills the reservoir up until sample_count entries, returns how many entries are still required
	idx_t FillReservoir(DataChunk &input);

	DataChunk &Chunk();

public:
	Allocator &allocator;
	//! The size of the reservoir sample.
	//! when calculating percentages, it is set to reservoir_threshold * percentage
	//! when explicit number used, sample_count = number
	idx_t sample_count;
	//! The current reservoir
	unique_ptr<ReservoirChunk> reservoir_chunk;
};

//! The reservoir sample sample_size class maintains a streaming sample of variable size
class ReservoirSamplePercentage : public BlockingSample {
	constexpr static idx_t RESERVOIR_THRESHOLD = 100000;

public:
	static constexpr const SampleType TYPE = SampleType::RESERVOIR_PERCENTAGE_SAMPLE;

public:
	ReservoirSamplePercentage(Allocator &allocator, double percentage, int64_t seed = -1);
	ReservoirSamplePercentage(double percentage, int64_t seed, idx_t reservoir_sample_size);
	explicit ReservoirSamplePercentage(double percentage, int64_t seed = -1);

	idx_t NumSamplesCollected() override;
	//! Add a chunk of data to the sample
	void AddToReservoir(DataChunk &input) override;

	//! create a new reservoir sample that has a fixes sample size of "sample_count"
	//! dump all samples into the new reservoir sample and return.
	//! This is used to serialize the Sample.
	unique_ptr<ReservoirSample> ConvertToFixedReservoirSample(idx_t sample_count);

	void Merge(unique_ptr<BlockingSample> other) override;

	unique_ptr<BlockingSample> Copy() const override;

	//! Fetches a chunk from the sample. Note that this method is destructive and should only be used after the
	//! sample is completely built.
	unique_ptr<DataChunk> GetChunkAndShrink() override;
	//! Fetches a chunk from the sample. This method is not destructive
	unique_ptr<DataChunk> GetChunk(idx_t offset = 0) override;
	void Finalize() override;
	void FromReservoirSample(unique_ptr<ReservoirSample> other);

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<BlockingSample> Deserialize(Deserializer &deserializer);

private:
	Allocator &allocator;
	//! The sample_size to sample
	double sample_percentage;
	//! The fixed sample size of the sub-reservoirs
	idx_t reservoir_sample_size;

	//! The current sample
	unique_ptr<ReservoirSample> current_sample;

	//! The set of finished samples of the reservoir sample
	vector<unique_ptr<ReservoirSample>> finished_samples;

	//! The amount of tuples that have been processed so far (not put in the reservoir, just processed)
	idx_t current_count = 0;
	//! Whether or not the stream is finalized. The stream is automatically finalized on the first call to
	//! GetChunkAndShrink();
	bool is_finalized;
};

} // namespace duckdb
