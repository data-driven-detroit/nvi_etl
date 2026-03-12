import pytest
import logging
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, box
from unittest.mock import MagicMock

from transform_data import (
    recode,
    recode_and_clean_columns,
    apply_spatial_joins,
    roll_up_indicators,
    roll_up_questions,
    AGGREGATION_LEVELS,
)


# ── Shared fixtures ──────────────────────────────────────────────────


@pytest.fixture
def logger():
    return logging.getLogger("test_transform")


@pytest.fixture
def city():
    return gpd.GeoDataFrame(
        {"geoid": ["2622000"]},
        geometry=[box(0, 0, 100, 100)],
        crs="EPSG:2898",
    )


@pytest.fixture
def districts():
    return gpd.GeoDataFrame(
        {"district_number": [1, 2]},
        geometry=[box(0, 0, 50, 100), box(50, 0, 100, 100)],
        crs="EPSG:2898",
    )


@pytest.fixture
def zones():
    return gpd.GeoDataFrame(
        {"zone_id": [1, 2]},
        geometry=[box(0, 0, 100, 50), box(0, 50, 100, 100)],
        crs="EPSG:2898",
    )


@pytest.fixture
def cdo_boundaries():
    return gpd.GeoDataFrame(
        {"organization_name": ["CDO_A", "CDO_B"]},
        geometry=[box(0, 0, 50, 50), box(50, 50, 100, 100)],
        crs="EPSG:2898",
    )


@pytest.fixture
def location_map():
    return {
        "citywide": {"2622000": 1},
        "district": {1: 101, 2: 102},
        "zone": {1: 201, 2: 202},
    }


@pytest.fixture
def sample_gdf():
    return gpd.GeoDataFrame(
        {
            "Response ID": [1, 2, 3],
            "successful_geocode": ["M", "M", "M"],
            "Q1": [4, 2, 5],
        },
        geometry=[Point(25, 25), Point(75, 75), Point(25, 75)],
        crs="EPSG:2898",
    )


# ── recode ───────────────────────────────────────────────────────────


class TestRecode:
    def test_integer_mapping(self, logger):
        df = pd.DataFrame({"Q1": [1, 2, 3]})
        result = recode(df, {"Q1": {"1": "10", "2": "20", "3": "30"}}, logger)
        assert list(result["Q1"]) == [10, 20, 30]

    def test_missing_column_warns(self, logger, caplog):
        df = pd.DataFrame({"Q1": [1]})
        with caplog.at_level(logging.WARNING, logger="test_transform"):
            recode(df, {"MISSING": {"1": "10"}}, logger)
        assert "MISSING not found" in caplog.text

    def test_preserves_nulls(self, logger):
        df = pd.DataFrame({"Q1": pd.array([1, pd.NA, 3], dtype=pd.Int64Dtype())})
        result = recode(df, {"Q1": {"1": "10", "3": "30"}}, logger)
        assert pd.isna(result["Q1"].iloc[1])

    def test_does_not_mutate_input(self, logger):
        df = pd.DataFrame({"Q1": [1, 2]})
        original = df.copy()
        recode(df, {"Q1": {"1": "10", "2": "20"}}, logger)
        pd.testing.assert_frame_equal(df, original)

    def test_string_passthrough(self, logger):
        """Values not in the recode map pass through unchanged."""
        df = pd.DataFrame({"Q1": ["yes", "no", "maybe"]})
        result = recode(df, {"Q1": {"yes": "1", "no": "0"}}, logger)
        assert result["Q1"].iloc[2] == "maybe"


# ── recode_and_clean_columns ─────────────────────────────────────────


class TestRecodeAndCleanColumns:
    def test_html_tag_removal(self, logger):
        df = pd.DataFrame({
            "Cleaned up or improved lot(s) <u>that I own</u>:In_The_Last_12_Months": [1],
            "Cleaned up or improved lot(s) <u>that I do <b>not</b> own</u>:In_The_Last_12_Months": [2],
        })
        result = recode_and_clean_columns(df, {}, logger)
        assert "Cleaned up or improved lot(s) that I own:In_The_Last_12_Months" in result.columns
        assert "Cleaned up or improved lot(s) that I do not own:In_The_Last_12_Months" in result.columns

    def test_double_colon_fix(self, logger):
        df = pd.DataFrame({"A::B": [1]})
        result = recode_and_clean_columns(df, {}, logger)
        assert "A:B" in result.columns
        assert "A::B" not in result.columns

    def test_curly_apostrophe(self, logger):
        df = pd.DataFrame({"it's": [1]})
        result = recode_and_clean_columns(df, {}, logger)
        assert "it\u2019s" in result.columns

    def test_school_program_rename(self, logger):
        df = pd.DataFrame({
            "SchoolProgram_Sports_Tutoring_Participation:Youth_In_Household_Last_12_Months_Questions4": [1],
        })
        result = recode_and_clean_columns(df, {}, logger)
        assert "SchoolProgram_Leadership_Participation:Youth_In_Household_Last_12_Months_Questions" in result.columns

    def test_recode_is_applied(self, logger):
        df = pd.DataFrame({"Q1": [1, 2]})
        result = recode_and_clean_columns(df, {"Q1": {"1": "10", "2": "20"}}, logger)
        assert list(result["Q1"]) == [10, 20]


# ── apply_spatial_joins ──────────────────────────────────────────────


class TestApplySpatialJoins:
    def test_adds_geography_columns(
        self, sample_gdf, city, districts, zones, cdo_boundaries, location_map, logger
    ):
        result = apply_spatial_joins(
            sample_gdf, city, districts, zones, cdo_boundaries, location_map, False, logger
        )
        for col in ["citywide", "district", "zone", "organization_name"]:
            assert col in result.columns

    def test_drops_geometry(
        self, sample_gdf, city, districts, zones, cdo_boundaries, location_map, logger
    ):
        result = apply_spatial_joins(
            sample_gdf, city, districts, zones, cdo_boundaries, location_map, False, logger
        )
        assert "geometry" not in result.columns

    def test_filters_failed_geocode(
        self, city, districts, zones, cdo_boundaries, location_map, logger
    ):
        gdf = gpd.GeoDataFrame(
            {
                "Response ID": [1, 2],
                "successful_geocode": ["M", "F"],
                "Q1": [4, 2],
            },
            geometry=[Point(25, 25), Point(75, 75)],
            crs="EPSG:2898",
        )
        result = apply_spatial_joins(
            gdf, city, districts, zones, cdo_boundaries, location_map, False, logger
        )
        assert len(result) == 1
        assert result.iloc[0]["Response ID"] == 1

    def test_dedup_without_cdo(
        self, sample_gdf, city, districts, zones, cdo_boundaries, location_map, logger
    ):
        result = apply_spatial_joins(
            sample_gdf, city, districts, zones, cdo_boundaries, location_map, False, logger
        )
        assert result["Response ID"].is_unique

    def test_dedup_with_cdo_keeps_per_org_rows(
        self, city, districts, zones, location_map, logger
    ):
        """With cdo_aggregate=True, a response in overlapping CDOs gets one row per CDO."""
        overlapping_cdos = gpd.GeoDataFrame(
            {"organization_name": ["CDO_X", "CDO_Y"]},
            geometry=[box(0, 0, 60, 60), box(0, 0, 60, 60)],
            crs="EPSG:2898",
        )
        gdf = gpd.GeoDataFrame(
            {
                "Response ID": [1],
                "successful_geocode": ["M"],
                "Q1": [4],
            },
            geometry=[Point(25, 25)],
            crs="EPSG:2898",
        )
        result = apply_spatial_joins(
            gdf, city, districts, zones, overlapping_cdos, location_map, True, logger
        )
        assert len(result) == 2
        assert set(result["organization_name"]) == {"CDO_X", "CDO_Y"}

    def test_warns_on_duplicate_rows(
        self, city, districts, zones, location_map, logger, caplog
    ):
        """Overlapping boundaries cause duplicates -> warning."""
        overlapping_cdos = gpd.GeoDataFrame(
            {"organization_name": ["CDO_X", "CDO_Y"]},
            geometry=[box(0, 0, 60, 60), box(0, 0, 60, 60)],
            crs="EPSG:2898",
        )
        gdf = gpd.GeoDataFrame(
            {
                "Response ID": [1],
                "successful_geocode": ["M"],
                "Q1": [4],
            },
            geometry=[Point(25, 25)],
            crs="EPSG:2898",
        )
        with caplog.at_level(logging.WARNING, logger="test_transform"):
            apply_spatial_joins(
                gdf, city, districts, zones, overlapping_cdos, location_map, False, logger
            )
        assert "duplicated row(s)" in caplog.text


# ── roll_up_indicators ───────────────────────────────────────────────


class TestRollUpIndicators:
    @staticmethod
    def _mock_compile(indicator_id, agg, readable=False):
        """Return a DataFrame shaped like a real compile result, index named after agg."""
        return pd.DataFrame(
            {"count": [5], "universe": [10], "percentage": [0.5]},
            index=pd.Index([1], name=agg),
        )

    def test_single_indicator(self):
        nvi = MagicMock()
        nvi.answer_key = pd.DataFrame({
            "indicator_db_id": [1.0],
            "response_type": ["SINGLE"],
        })
        nvi.compile_single_response_indicator.side_effect = self._mock_compile

        result = roll_up_indicators(nvi, 2024)

        assert nvi.compile_single_response_indicator.call_count == len(AGGREGATION_LEVELS)
        assert "location_id" in result.columns
        assert "indicator_id" in result.columns
        assert (result["value_type_id"] == 1).all()

    def test_multi_select_indicator(self):
        nvi = MagicMock()
        nvi.answer_key = pd.DataFrame({
            "indicator_db_id": [2.0],
            "response_type": ["MULTI-SELECT"],
        })
        nvi.compile_multi_response_indicator.side_effect = self._mock_compile

        result = roll_up_indicators(nvi, 2024)

        assert nvi.compile_multi_response_indicator.call_count == len(AGGREGATION_LEVELS)
        assert (result["value_type_id"] == 1).all()

    def test_mixed_indicators(self):
        nvi = MagicMock()
        nvi.answer_key = pd.DataFrame({
            "indicator_db_id": [1.0, 2.0],
            "response_type": ["SINGLE", "MULTI-SELECT"],
        })
        nvi.compile_single_response_indicator.side_effect = self._mock_compile
        nvi.compile_multi_response_indicator.side_effect = self._mock_compile

        result = roll_up_indicators(nvi, 2024)

        assert nvi.compile_single_response_indicator.call_count == len(AGGREGATION_LEVELS)
        assert nvi.compile_multi_response_indicator.call_count == len(AGGREGATION_LEVELS)
        assert len(result) == 2 * len(AGGREGATION_LEVELS)

    def test_invalid_response_type_raises(self):
        nvi = MagicMock()
        nvi.answer_key = pd.DataFrame({
            "indicator_db_id": [1.0],
            "response_type": ["INVALID"],
        })
        with pytest.raises(ValueError, match="not a valid response type"):
            roll_up_indicators(nvi, 2024)

    def test_skips_rows_without_indicator_id(self):
        nvi = MagicMock()
        nvi.answer_key = pd.DataFrame({
            "indicator_db_id": [1.0, np.nan],
            "response_type": ["SINGLE", "SINGLE"],
        })
        nvi.compile_single_response_indicator.side_effect = self._mock_compile

        roll_up_indicators(nvi, 2024)

        # Only 1 indicator (the NaN row should be dropped)
        assert nvi.compile_single_response_indicator.call_count == len(AGGREGATION_LEVELS)

    def test_uses_all_aggregation_levels(self):
        nvi = MagicMock()
        nvi.answer_key = pd.DataFrame({
            "indicator_db_id": [1.0],
            "response_type": ["SINGLE"],
        })
        nvi.compile_single_response_indicator.side_effect = self._mock_compile

        roll_up_indicators(nvi, 2024)

        called_aggs = [
            call.args[1]
            for call in nvi.compile_single_response_indicator.call_args_list
        ]
        assert called_aggs == AGGREGATION_LEVELS


# ── roll_up_questions ────────────────────────────────────────────────


class TestRollUpQuestions:
    @pytest.fixture
    def nvi_with_single(self):
        """Minimal Survey-like object with one SINGLE-response question."""
        survey_data = pd.DataFrame({
            "Response_ID": range(1, 11),
            "citywide": pd.array([1] * 10, dtype=pd.Int64Dtype()),
            "district": pd.array([101] * 5 + [102] * 5, dtype=pd.Int64Dtype()),
            "zone": pd.array([201] * 5 + [202] * 5, dtype=pd.Int64Dtype()),
            "organization_name": ["CDO_A"] * 5 + ["CDO_B"] * 5,
            "Neighborhood_Satisfaction": pd.array(
                [4, 2, 5, 3, 4, 1, 5, 3, 4, 2], dtype=pd.Int64Dtype()
            ),
        })

        answer_key = pd.DataFrame({
            "indicator_db_id": pd.array([1, 1, 1, 1, 1], dtype=pd.Int64Dtype()),
            "group": ["Satisfaction"] * 5,
            "question": ["Neighborhood_Satisfaction"] * 5,
            "full_column": ["Neighborhood_Satisfaction"] * 5,
            "response_type": ["SINGLE"] * 5,
            "survey_code": pd.array([1, 2, 3, 4, 5], dtype=pd.Int64Dtype()),
            "db_question_code": pd.array([100, 100, 100, 100, 100], dtype=pd.Int64Dtype()),
            "db_answer_code": pd.array([1001, 1002, 1003, 1004, 1005], dtype=pd.Int64Dtype()),
            "universe_query": ["Response_ID > 0"] * 5,
            "indicator_include": [True] * 5,
            "tabulate": [True] * 5,
        })

        nvi = MagicMock()
        nvi.answer_key = answer_key
        nvi.survey_data = survey_data
        return nvi

    @pytest.fixture
    def nvi_with_multi(self):
        """Minimal Survey-like object with one MULTI-SELECT question."""
        survey_data = pd.DataFrame({
            "Response_ID": range(1, 11),
            "citywide": pd.array([1] * 10, dtype=pd.Int64Dtype()),
            "district": pd.array([101] * 5 + [102] * 5, dtype=pd.Int64Dtype()),
            "zone": pd.array([201] * 5 + [202] * 5, dtype=pd.Int64Dtype()),
            "organization_name": ["CDO_A"] * 5 + ["CDO_B"] * 5,
            "Bus:Transportation_Used": pd.array(
                [1, pd.NA, pd.NA, 1, 1, pd.NA, pd.NA, pd.NA, pd.NA, 1], dtype=pd.Int64Dtype()
            ),
            "Bike:Transportation_Used": pd.array(
                [pd.NA, 1, 1, pd.NA, 1, pd.NA, pd.NA, 1, 1, pd.NA], dtype=pd.Int64Dtype()
            ),
            "Car:Transportation_Used": pd.array(
                [1, 1, pd.NA, pd.NA, pd.NA, 1, 1, pd.NA, 1, pd.NA], dtype=pd.Int64Dtype()
            ),
        })

        answer_key = pd.DataFrame({
            "indicator_db_id": pd.array([3, 3, 3], dtype=pd.Int64Dtype()),
            "group": ["Transportation_Used"] * 3,
            "question": ["Bus", "Bike", "Car"],
            "full_column": [
                "Bus:Transportation_Used",
                "Bike:Transportation_Used",
                "Car:Transportation_Used",
            ],
            "response_type": ["MULTI-SELECT"] * 3,
            "survey_code": pd.array([1, 1, 1], dtype=pd.Int64Dtype()),
            "db_question_code": pd.array([300, 301, 302], dtype=pd.Int64Dtype()),
            "db_answer_code": pd.array([3001, 3002, 3003], dtype=pd.Int64Dtype()),
            "universe_query": ["Response_ID > 0"] * 3,
            "indicator_include": [True] * 3,
            "tabulate": [True] * 3,
        })

        nvi = MagicMock()
        nvi.answer_key = answer_key
        nvi.survey_data = survey_data
        return nvi

    def test_single_question_output_columns(self, nvi_with_single):
        result = roll_up_questions(nvi_with_single, 2024)
        for col in [
            "location_id", "indicator_id", "survey_question_id",
            "survey_question_option_id", "count", "universe", "percentage",
        ]:
            assert col in result.columns
        assert (result["value_type_id"] == 2).all()

    def test_single_question_all_agg_levels_present(self, nvi_with_single):
        result = roll_up_questions(nvi_with_single, 2024)
        # citywide=1, district=101/102, zone=201/202, org=CDO_A/CDO_B
        location_ids = set(result["location_id"])
        assert 1 in location_ids        # districts
        assert 101 in location_ids
        assert 102 in location_ids

    def test_single_question_percentages_valid(self, nvi_with_single):
        result = roll_up_questions(nvi_with_single, 2024)
        assert (result["percentage"] >= 0).all()
        assert (result["percentage"] <= 1).all()

    def test_multi_select_output_columns(self, nvi_with_multi):
        result = roll_up_questions(nvi_with_multi, 2024)
        for col in [
            "location_id", "indicator_id", "survey_question_id",
            "survey_question_option_id", "count", "universe", "percentage",
        ]:
            assert col in result.columns
        assert (result["value_type_id"] == 2).all()

    def test_multi_select_percentages_valid(self, nvi_with_multi):
        result = roll_up_questions(nvi_with_multi, 2024)
        assert (result["percentage"] >= 0).all()
        assert (result["percentage"] <= 1).all()

    def test_invalid_response_type_raises(self):
        nvi = MagicMock()
        nvi.answer_key = pd.DataFrame({
            "indicator_db_id": pd.array([1], dtype=pd.Int64Dtype()),
            "group": ["G"],
            "question": ["Q"],
            "response_type": ["INVALID"],
            "indicator_include": [True],
        })
        with pytest.raises(ValueError, match="not a valid response type"):
            roll_up_questions(nvi, 2024)
