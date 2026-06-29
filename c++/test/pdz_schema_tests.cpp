#include "pod5_format/signal_table_schema.h"
#include "pod5_format/types.h"
#include "test_utils.h"

#include <arrow/extension_type.h>
#include <arrow/type.h>
#include <catch2/catch.hpp>
#include <gsl/gsl-lite.hpp>

// Exercises the `minknow.pdz` extension type and its signal-table schema
// dispatch, mirroring the existing VBZ behaviour. No signal data is read/written
// here.

SCENARIO("PDZ signal table schema round-trips")
{
    using namespace pod5;

    REQUIRE_ARROW_STATUS_OK(register_extension_types());
    auto unregister = gsl::finally([] { (void)unregister_extension_types(); });

    GIVEN("A schema built for SignalType::PdzSignal")
    {
        SignalTableSchemaDescription built_locations;
        auto const schema = make_signal_table_schema(SignalType::PdzSignal, nullptr, &built_locations);
        REQUIRE(schema != nullptr);

        THEN("The signal field uses the minknow.pdz extension type")
        {
            auto const signal_field = schema->field(built_locations.signal);
            REQUIRE(signal_field->name() == "signal");
            REQUIRE(signal_field->type()->Equals(pdz_signal()));
            REQUIRE_FALSE(signal_field->type()->Equals(vbz_signal()));
        }

        THEN("Reading the schema back recovers the PdzSignal type and field indices")
        {
            auto const read_locations = read_signal_table_schema(schema);
            REQUIRE_ARROW_STATUS_OK(read_locations);
            CHECK(read_locations->signal_type == SignalType::PdzSignal);
            CHECK(read_locations->read_id == built_locations.read_id);
            CHECK(read_locations->signal == built_locations.signal);
            CHECK(read_locations->samples == built_locations.samples);
            CHECK(read_locations->read_id == 0);
            CHECK(read_locations->signal == 1);
            CHECK(read_locations->samples == 2);
        }
    }
}

SCENARIO("PDZ extension type registration and identity")
{
    using namespace pod5;

    GIVEN("Registered extension types")
    {
        REQUIRE_ARROW_STATUS_OK(register_extension_types());
        auto unregister = gsl::finally([] { (void)unregister_extension_types(); });

        THEN("minknow.pdz resolves from the arrow registry")
        {
            REQUIRE(arrow::GetExtensionType("minknow.pdz") != nullptr);
        }
    }

    GIVEN("A PdzSignalType instance")
    {
        auto const pdz = pdz_signal();

        THEN("It has the expected on-disk identity")
        {
            CHECK(pdz->extension_name() == "minknow.pdz");
            CHECK(pdz->storage_type()->Equals(*arrow::large_binary()));
        }

        THEN("ExtensionEquals is true vs another PdzSignalType, false vs vbz")
        {
            PdzSignalType const other;
            CHECK(pdz->ExtensionEquals(other));
            CHECK(other.ExtensionEquals(*pdz));
            CHECK_FALSE(pdz->ExtensionEquals(*vbz_signal()));
            CHECK_FALSE(vbz_signal()->ExtensionEquals(*pdz));
        }

        THEN("Serialize emits no metadata")
        {
            CHECK(pdz->Serialize() == "");
        }

        THEN("Deserialize accepts large_binary storage with empty metadata")
        {
            auto const result = pdz->Deserialize(arrow::large_binary(), "");
            REQUIRE_ARROW_STATUS_OK(result);
            CHECK((*result)->Equals(*pdz));
        }

        THEN("Deserialize rejects non-empty metadata")
        {
            CHECK_ARROW_STATUS_NOT_OK(pdz->Deserialize(arrow::large_binary(), "unexpected"));
        }

        THEN("Deserialize rejects the wrong storage type")
        {
            CHECK_ARROW_STATUS_NOT_OK(pdz->Deserialize(arrow::binary(), ""));
            CHECK_ARROW_STATUS_NOT_OK(pdz->Deserialize(arrow::utf8(), ""));
        }
    }
}
