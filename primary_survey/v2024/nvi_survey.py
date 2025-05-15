from pathlib import Path
from dataclasses import dataclass
import pandas as pd

from nvi_etl import make_engine_for
from sqlalchemy import text


ALL = tuple() # This is to reference in calls to '.query()'

WORKING_DIR = Path(__file__).parent


@dataclass
class Survey:
    # The survey data is recoded and geocoded with the geographies of interest included
    survey_data: pd.DataFrame

    # The answer key is an answer-level table, so multiple rows for each question
    # unless that question is a numeric response or a free-text field
    answer_key: pd.DataFrame

    # This maps indicator ids back to the text describing indicator
    indicator_key: dict[int, str]

    @property
    def questions(self):
        return self.answer_key[["group", "question"]].drop_duplicates()

    def top_of_q(self, question, question_group=None):
        try:
            question_meta = self.answer_key[
                (self.answer_key["question"] == question)
                & (self.answer_key["group"] == question_group)
            ].iloc[0]

            return question_meta

        except IndexError:
            raise IndexError(f"No rows with question '{question}' and question group '{question_group}' found in the answer key.")

    def make_renamer(self, question, question_group, ignore=list()):
        column_rename = {}

        for _, row in self.answer_key[
            (self.answer_key["group"] == question_group)
            & (self.answer_key["question"] == question)
        ].iterrows():
            if row["survey_code"] not in ignore:
                column_rename[row["survey_code"]] = row["answer"]
            else:
                column_rename[row["survey_code"]] = row["survey_code"]

        return lambda v: column_rename.get(v, "Not answered")
    
    def make_col_to_answer_renamer(self, question_group, question, ignore=list()):
        """
        This is similar to the regular renamer, but is used in the 'grid of death'
        tabulation.
        """
        answers = self.answer_key[
            (self.answer_key["group"] == question_group)
            & (self.answer_key["question"] == question)
        ]

        if len(answers) == 0:
            raise KeyError(
                f"No rows for renamer columns found for group '{question_group}, and question '{question}'"
            )

        mapper = {
            row["full_column"]: row["answer"]
            for _, row in answers.iterrows()
        }

        for ignorable in ignore:
            mapper[ignorable] = ignorable

        return lambda v: mapper.get(v, "No alias for this column")

    def tabulate_question(self, question, question_group=None, group_var=None):
        """
        This will look up the question type in the answer key and decide the best
        strategy for aggregation. It also has ignorable group var if you just
        want to show all the responses on a single line.
        """

        question_meta = self.top_of_q(question, question_group)

        if question_meta["response_type"] in ("SINGLE", "GROUPED-SINGLE"):
            return self.tabulate_single_question(group_var, question, question_group)

        if question_meta["response_type"] == "MULTI-SELECT":
            return self.tabulate_multiselect(group_var, question, question_group)
        
        if question_meta["response_type"] == "GRID-OF-DEATH":
            raise NotImplementedError("For the 'GRID-OF-DEATH' you have to call .tabulate_grid_of_death() for now.")
        
        raise ValueError(f"{question_meta["response_type"]} not a valid response-type for tabulation.")

    def tabulate_single_question(self, group_var, question, question_group=None, readable=True):
        if not question_group:
            question_group = question

        question_meta = self.top_of_q(question, question_group)

        if readable:
            index_renamer = self.make_renamer(group_var, group_var)
        else:
            index_renamer = {}

        column_renamer = self.make_renamer(question, question_group)

        return (
            self.survey_data[[group_var, question_meta["full_column"]]]
            .astype({
                group_var: pd.Int64Dtype(),
                question_meta["full_column"]: pd.Int64Dtype(),
            })
            .value_counts()
            .reset_index()
            .pivot(
                columns=question_meta["full_column"],
                index=group_var,
                values="count"
            )
            .rename(
                columns=column_renamer, 
                index=index_renamer
            )
            .assign(**{"Total Responses": lambda df: df.sum(axis=1)})
            .fillna(0)
        )

    def tabulate_multiselect(self, group_var, question_group, readable=True):
        questions = self.answer_key[
            self.answer_key["group"] == question_group
        ]

        if readable:
            index_renamer = self.make_renamer(group_var, group_var)
        else:
            index_renamer = {}


        aggregations = []
        for _, question in questions.iterrows():
            try:
                aggregations.append(
                    self.survey_data
                    .groupby(group_var, dropna=False)[question["full_column"]]
                    .count()
                    .rename(question["question"])
                )
            except KeyError:
                print(f"{question["full_column"]} missing")

        aggregations.append(
            self.survey_data
            .groupby(group_var, dropna=False)
            .size()
            .rename("Total Responses")
        )

        return (
            pd.concat(aggregations, axis=1)
            .rename(index_renamer)
        )

    def tabulate_grid_of_death(self, group_var, question_group, readable=True):
        dig_eq_rows = self.answer_key[self.answer_key["group"] == question_group]

        if readable:
            index_renamer = self.make_renamer(group_var, group_var)
        else:
            index_renamer = {}

        frames = []
        for q, q_group in dig_eq_rows.groupby("question"):

            column_renamer = self.make_col_to_answer_renamer(
                "DigitalEquity_Sources_Information", q, ignore=["Question", "Total Responses"]
            )

            rolled = (
                self.survey_data[[group_var] + list(q_group["full_column"])]
                .groupby(group_var)
                .count()
            )

            rolled["Total Responses"] = self.survey_data.groupby(group_var).size()
            rolled.insert(0, "Question", q)

            frames.append(rolled.rename(index=index_renamer, columns=column_renamer))

        return pd.concat(frames)

    def compile_single_response_indicator(self, indicator_id, group_var, readable=True):
        indicator_rows = self.answer_key[
            self.answer_key["indicator_db_id"] == indicator_id
        ]

        relevant_columns = indicator_rows["full_column"].drop_duplicates()


        indicator_meta = indicator_rows.iloc[0]
        universe = self.survey_data.query(indicator_meta["universe_query"])

        if readable:
            index_renamer = self.make_renamer(group_var, group_var)
        else:
            index_renamer = {}

        accepted_values = {
            column: list(answers[answers["indicator_include"]]["survey_code"])
            for column, answers in indicator_rows.groupby("full_column")
        }

        index_include = (
            universe[relevant_columns]
            .isin(accepted_values)
        )

        intermediate = pd.concat([index_include, universe[group_var]], axis=1)

        if indicator_meta["and_or"] == "AND":
            combo_strategy = lambda df: df[indicator_rows["full_column"].drop_duplicates()].all(axis=1)
        elif indicator_meta["and_or"] == "OR":
            combo_strategy = lambda df: df[indicator_rows["full_column"].drop_duplicates()].any(axis=1)
        else:
            raise ValueError(f"{indicator_meta["and_or"]} is not a valid value for 'and_or' for indicator {indicator_id}")

        return (
            intermediate
            .dropna(subset=relevant_columns, how="all") # This is different from how multi-select universe is calculated
            .assign(included=combo_strategy)
            .groupby(group_var)
            .aggregate(
                count=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum()),
                universe=pd.NamedAgg(column="included", aggfunc=lambda c: c.count()),
                percentage=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum() / c.count()),
            )
            .rename(
                index=index_renamer,
            )
        )

    def compile_multi_response_indicator(self, indicator_id, group_var):
        indicator_rows = self.answer_key[
            (self.answer_key["indicator_db_id"] == indicator_id)
            & self.answer_key["indicator_include"]
        ]

        indicator_meta = indicator_rows.iloc[0]
        universe = self.survey_data.query(indicator_meta["universe_query"])

        index_renamer = self.make_renamer(group_var, group_var)

        labeled = pd.concat([
            universe[group_var],
            ~universe[indicator_rows["full_column"]].isna()
        ], axis=1)

        if indicator_meta["and_or"] == "AND":
            combo_strategy = lambda df: df[indicator_rows["full_column"].drop_duplicates()].all(axis=1)
        elif indicator_meta["and_or"] == "OR":
            combo_strategy = lambda df: df[indicator_rows["full_column"].drop_duplicates()].any(axis=1)
        else:
            raise ValueError(f"{indicator_meta["and_or"]} is not a valid value for 'and_or' for indicator {indicator_id}")

        return (
            labeled
            .assign(included=combo_strategy)
            .groupby(group_var)
            .aggregate(
                count=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum()),
                universe=pd.NamedAgg(column="included", aggfunc=lambda c: c.count()),
                percentage=pd.NamedAgg(column="included", aggfunc=lambda c: c.sum() / c.count()),
            )
            .rename(
                index=index_renamer,
            )
        )


def create_nvi_survey(filepath):
    nvi_engine = make_engine_for("nvi_test")

    answer_key = pd.read_excel(
        WORKING_DIR / "conf" / "nvi_answer_key.xlsx", 
        dtype={
            "indicator_db_id": pd.Int64Dtype(),
            "survey_code": pd.Int64Dtype(),
        }
    )

    recoded = pd.read_csv(
        filepath,
        low_memory=False,
        dtype={
            col: pd.Int64Dtype()
            for col in answer_key[answer_key["tabulate"]]["full_column"].drop_duplicates()
        }
    )

    q = text("""
    SELECT *
    FROM indicator;
    """)

    indicators = pd.read_sql(q, nvi_engine)

    indicator_key = {
        row["id"]: row["name"]
        for _, row in indicators.iterrows()
    }

    return Survey(recoded, answer_key, indicator_key)