#pragma once

#include <cmath>
#include <type_traits>
#include <experimental/type_traits>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Common/FieldVisitorConvertToNumber.h>




namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

template <typename ValueType, typename TimestampType>
struct AggregationFunctionExpMovingSumData
{
    Float64 sum = 0;
    TimestampType ts = 0;
    bool initialized = false;
};


template <typename ValueType, typename TimestampType>
class AggregationFunctionExpMovingSum final
    : public IAggregateFunctionDataHelper<
        AggregationFunctionExpMovingSumData<ValueType, TimestampType>,
        AggregationFunctionExpMovingSum<ValueType, TimestampType>
      >
{
    const Float64 period;
    
    static Float64 getSummingPeriod(const Array & params) 
    {
        if(params.empty())
            return 1.0;

        if(params.size() > 1)
            throw Exception("One or zero parameter values expected for averaging period",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);


        return applyVisitor(FieldVisitorConvertToNumber<Float64>(), params[0]);
    }

public:
    AggregationFunctionExpMovingSum(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<
            AggregationFunctionExpMovingSumData<ValueType, TimestampType>,
            AggregationFunctionExpMovingSum<ValueType, TimestampType>
        >{arguments, params}, period(getSummingPeriod(params))
    {}

    AggregationFunctionExpMovingSum()
        : IAggregateFunctionDataHelper<
            AggregationFunctionExpMovingSumData<ValueType, TimestampType>,
            AggregationFunctionExpMovingSum<ValueType, TimestampType>
        >{}
    {}


    bool allocatesMemoryInArena() const override { return false; }

    String getName() const override { return "expMovingSum"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeFloat64>(); }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
            
        Float64 value = assert_cast<const ColumnVector<ValueType> &>(*columns[0]).getData()[row_num];
        auto ts = assert_cast<const ColumnVector<TimestampType> &>(*columns[1]).getData()[row_num];


        if(!this->data(place).initialized) {
            this->data(place).sum = value;
            this->data(place).initialized = true;
        } else {
            Int64 delta = ts - this->data(place).ts;
            this->data(place).sum = value + std::exp(-delta / period) * this->data(place).sum;
        } 
        this->data(place).ts = ts;
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        using Data = AggregationFunctionExpMovingSumData<ValueType, TimestampType>;
        const Data *first;
        const Data *second;

        if(this->data(place).ts < this->data(rhs).ts) {
            first = &this->data(place);
            second = &this->data(rhs);
        }
        else {
            first = &this->data(rhs);
            second = &this->data(place);
        }

        Int64 delta = second->ts - first->ts;
        this->data(place).sum = second->sum + std::exp(-delta / period) * first->sum;
        this->data(place).ts = second->ts;

    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        writeFloatBinary(this->data(place).sum, buf);
        writeIntBinary(this->data(place).ts, buf);
        writePODBinary(this->data(place).initialized, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        readFloatBinary(this->data(place).sum, buf);
        readIntBinary(this->data(place).ts, buf);
        readPODBinary(this->data(place).initialized, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnFloat64 &>(to).getData().push_back(this->data(place).sum);
    }
};

}
