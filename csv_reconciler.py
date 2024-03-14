import argparse
from pathlib import Path

import fuzzy_pandas as fpd
import numpy as np
import pandas as pd

import warnings

warnings.filterwarnings(action="ignore")


def get_mismatched_records_reconciliation(report: pd.DataFrame) -> pd.DataFrame:
    mismatched_records = report[
        report["Type"].isin(["Missing in Source", "Missing in Target"])
    ]
    mismatch_reconciliation_report = mismatched_records[["Type", "Record Identifier"]]
    mismatch_reconciliation_report["Field"] = ""
    mismatch_reconciliation_report["Source Value"] = ""
    mismatch_reconciliation_report["Target Value"] = ""
    return mismatch_reconciliation_report


def fuzzy_match_non_identical_records(
    report: pd.DataFrame,
    source: pd.DataFrame,
    target: pd.DataFrame,
    fuzzy_merge_output: str,
    id_col: str,
    common_columns: list,
) -> None:
    source_only_record_ids = report[report["Type"] == "Missing in Target"][
        "Record Identifier"
    ].unique()
    source_only_df = source[source[id_col].isin(source_only_record_ids)]
    target_only_record_ids = report[report["Type"] == "Missing in Source"][
        "Record Identifier"
    ].unique()
    target_only_df = target[target[id_col].isin(target_only_record_ids)]
    for column in common_columns:
        source_only_df[column] = source_only_df[column].astype(str)
        target_only_df[column] = target_only_df[column].astype(str)
    source_only_df = source_only_df.rename(
        columns={column: f"source_{column}" for column in source_only_df.columns}
    )
    target_only_df = target_only_df.rename(
        columns={column: f"target_{column}" for column in target_only_df.columns}
    )
    fuzzy_report_df = fpd.fuzzy_merge(
        source_only_df,
        target_only_df,
        left_on=[f"source_{column}" for column in common_columns],
        right_on=[f"target_{column}" for column in common_columns],
        method="levenshtein",
        threshold=0.3,
    )
    fuzzy_report_df.to_csv(fuzzy_merge_output, index=False)


def get_discrepancies_report(
    matched_report: pd.DataFrame, common_columns: list
) -> pd.DataFrame:
    reconciliation_reports = []
    for column in common_columns:
        right_column = f"{column}_right_suffix"
        modified_col = f"modified_{column}"
        modified_right_col = f"modified_{column}_right_suffix"
        field_report = matched_report.copy()
        field_report[modified_col] = (
            field_report[column].astype(str).str.lower().str.strip()
        )
        field_report[modified_right_col] = (
            field_report[right_column].astype(str).str.lower().str.strip()
        )
        try:
            field_report[modified_col] = pd.to_datetime(field_report[modified_col])
            field_report[modified_right_col] = pd.to_datetime(
                field_report[modified_right_col]
            )
        except Exception:
            pass
        field_report[f"{modified_col}_field_match"] = field_report[modified_col].where(
            field_report[modified_col] == field_report[modified_right_col],
            "do not match",
        )
        field_report = field_report[
            field_report[f"{modified_col}_field_match"] == "do not match"
        ]
        field_report["Field"] = column
        field_report["Source Value"] = field_report[column]
        field_report["Target Value"] = field_report[right_column]
        field_report = field_report[
            [
                "Record Identifier",
                "Type",
                "Field",
                "Source Value",
                "Target Value",
            ]
        ]
        reconciliation_reports.append(field_report)
    return pd.concat(reconciliation_reports)


def get_input_dataframes(source: str, target: str) -> tuple:
    try:
        if not Path(source).is_file():
            print(f"{source} does not exist, specify the correct path for the source")
            return
        if not Path(target).is_file():
            print(f"{target} does not exist, specify the correct path for the target")
            return
        return pd.read_csv(source, dtype=str), pd.read_csv(target, dtype=str)
    except pd.errors.ParserError as e:
        print("Wrong file format provided")
    except Exception as e:
        print(e)


def get_columns_to_compare(source_columns: list, target_columns: list, columns: list):
    if columns:
        if source_columns[0] in columns or target_columns[0] in columns:
            return
        if set(columns).issubset(set(source_columns)) and set(columns).issubset(
            set(target_columns)
        ):
            return columns
        return
    return np.intersect1d(source_columns[1:], target_columns[1:]).tolist()


def main(
    source: str, target: str, output: str, columns: list, fuzzy_merge_output: bool
):
    """
    Reconciles the source and the target files
    """
    input_dfs = get_input_dataframes(source, target)
    if not input_dfs:
        return
    source, target = input_dfs

    common_columns = get_columns_to_compare(source.columns, target.columns, columns)
    if not common_columns:
        print("No valid columns available for comparison")
        return

    id_col = source.columns[0]

    report = source.merge(
        target,
        on=[id_col],
        how="outer",
        suffixes=("", "_right_suffix"),
        indicator=True,
    )
    report = report.rename(columns={"_merge": "Type", id_col: "Record Identifier"})
    report["Type"] = report["Type"].map(
        {
            "left_only": "Missing in Target",
            "right_only": "Missing in Source",
            "both": "Field Discrepancy",
        }
    )
    matched_report = report[report["Type"] == "Field Discrepancy"]
    field_discrepancies_report = get_discrepancies_report(
        matched_report, common_columns
    )
    records_with_discrepancies = len(
        field_discrepancies_report["Record Identifier"].unique()
    )
    mismatch_reconciliation_report = get_mismatched_records_reconciliation(report)
    reconciliation_report = pd.concat(
        [field_discrepancies_report, mismatch_reconciliation_report]
    )
    print(
        "Records missing in target: ",
        len(
            mismatch_reconciliation_report[
                mismatch_reconciliation_report["Type"] == "Missing in Target"
            ]
        ),
    )
    print(
        "Records missing in source: ",
        len(
            mismatch_reconciliation_report[
                mismatch_reconciliation_report["Type"] == "Missing in Source"
            ]
        ),
    )
    print("Records with field discrepancies: ", records_with_discrepancies)
    reconciliation_report.to_csv(output, index=False)
    print("Report saved to:", output)
    if fuzzy_merge_output:
        print("Running fuzzy merge.....")
        fuzzy_match_non_identical_records(
            report, source, target, fuzzy_merge_output, id_col, common_columns
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--source",
        type=str,
        help="The path of the first file to reconcile with the target",
    )
    parser.add_argument(
        "-t",
        "--target",
        type=str,
        help="The path of the second file to reconcile with the source",
    )
    parser.add_argument(
        "-o", "--output", type=str, help="The path to write the reconcilation report to"
    )
    parser.add_argument("--column", action="append")
    parser.add_argument("--fuzzy_merge_output", type=str, required=False)

    args = parser.parse_args()
    main(args.source, args.target, args.output, args.column, args.fuzzy_merge_output)
