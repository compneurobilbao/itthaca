import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import os

    import polars as pl

    return os, pl


@app.cell
def _(os):
    root_dir = os.path.abspath("../")
    data_dir = os.path.join(root_dir, "data")
    return (data_dir,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Load main CITA data
    """)
    return


@app.cell
def _(data_dir, os, pl):
    df_main_0m = pl.read_excel(os.path.join(data_dir,
                            "ITT_SHV_CITA-ALZHEIMER_BD-BASAL_sindr_dx.xls"))
    return (df_main_0m,)


@app.cell
def _(data_dir, os, pl):
    df_main_12m = pl.read_excel(os.path.join(data_dir,
                            "ITT_SHV_CITA-ALZHEIMER_BD-12M.xls"))
    return (df_main_12m,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Concatenate the two CITA dataframes
    """)
    return


@app.cell
def _(df_main_0m):
    codcita_id_svh_map = dict(zip(df_main_0m["record_id_SVH"], df_main_0m["codcita"]))
    return


@app.cell
def _(df_main_0m, df_main_12m, pl):
    # Find the set of matching columns of both dataframes
    matching_columns = set(df_main_0m.columns).intersection(set(df_main_12m.columns))

    # Find the ones that do not match
    non_matching_columns_0m = set(df_main_0m.columns) - matching_columns
    non_matching_columns_12m = set(df_main_12m.columns) - matching_columns

    # Looks like the "_12M" termination of the df_main_12m is the thing causing the mismatch.
    # Remove it
    df_main_12m_renamed = df_main_12m.rename({col: col.replace("_12M", "") for col in df_main_12m.columns})
    df_main_12m_renamed = df_main_12m_renamed.rename({col: col.replace("ñ", "n") for col in df_main_12m_renamed.columns})

    # Also the "vbasal" termination of the df_main_0m is the thing causing the mismatch. Remove it.
    df_main_0m_renamed = df_main_0m.rename({col: col.replace("_vbasal", "") for col in df_main_0m.columns})

    # NOTE: Communicate the ppl in cita that changing the terminations for each timepoint is a very messy way to do it,
    # and that it would be better to have a consistent naming convention across timepoints, e.g., by using a "timepoint"
    # column instead of encoding the timepoint in the column names.

    # Check again the matching and non-matching columns
    matching_columns = set(df_main_0m_renamed.columns).intersection(set(df_main_12m_renamed.columns))
    non_matching_columns_0m = set(df_main_0m_renamed.columns) - matching_columns # Columns in df_main_0m that are not in df_main_12m (important)
    non_matching_columns_12m = set(df_main_12m_renamed.columns) - matching_columns  # Columns in df_main_12m that are not in df_main_0m (can be ignored)

    # We will keep the matching columns AND the ones in df_main_0m that are not in df_main_12m, but we will ignore the
    # ones in df_main_12m that are not in df_main_0m.

    print(f"Matching columns (n:{len(matching_columns)}): {matching_columns}")
    print(f"Non-matching columns in 0m: {non_matching_columns_0m}")
    print(f"Non-matching columns in 12m: {non_matching_columns_12m}")

    columns_to_keep = list(set(matching_columns.union(non_matching_columns_0m)))
    # Concatenate the two dataframes diagonally, keeping the specified columns.
    # The columns that are not in one of the dataframes will be filled with null values.
    df_main = pl.concat(
        [
            df_main_0m_renamed.select(columns_to_keep),
            df_main_12m_renamed, #.select(columns_to_keep),
        ],
        how="diagonal_relaxed",
    ).select(columns_to_keep).with_columns(
        pl.when(
            pl.col("redcap_event_name") == "event_1_arm_1"
        ).then(pl.lit("0")).when(
            pl.col("redcap_event_name") == "visita_12m_arm_1"
        ).then(pl.lit("12"))
        .alias("timepoint")
    )

    print(df_main.head(3))
    print(df_main.shape)
    return (df_main,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Load proteomics data
    """)
    return


@app.cell
def _(data_dir, os, pl):
    df_proteomics = pl.read_csv(os.path.join(data_dir, "proteomics_preprocessed.csv"))
    df_codes_proteomics = pl.read_excel(os.path.join(data_dir, 
                                        "codigos_muestras_cita_atxukarro.xlsx")).drop(
                                            ["idx", "PROYECTO"]).rename(
                                                {"SUBJ_ID": "participant_id"}
                                            )

    df_proteomics = df_proteomics.with_columns(
        pl.col("participant_id").str.replace("_", "")
    ).join(
        df_codes_proteomics, on="participant_id", how="inner"
    )
    df_proteomics = df_proteomics.rename(
        {"CODIGO": "codigo_muestra_plasma_v0"}
    ).with_columns(
        pl.lit("0").alias("timepoint")
    )

    print(df_proteomics.head(3))
    return (df_proteomics,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Load acceXible data
    """)
    return


@app.cell
def _(data_dir, os, pl):
    df_accexible = pl.read_csv(os.path.join(data_dir, "svf_predictions_cita_id.csv"),
                               has_header=True, separator=",").rename(
                                {"participant_id": "participant_id_accexible",
                                 "cita_alzheimer_short_id": "codvoz",
                                 "diagnosis_original": "diagnosis_original_accexible",
                                 "prediction_prob": "prediction_prob_accexible",
                                 "level": "level_accexible"}
                               ).with_columns(pl.lit("0").alias("timepoint"))
    print(df_accexible.head(3))
    return (df_accexible,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Join the proteomics data with the main dataframe
    """)
    return


@app.cell
def _(data_dir, os, pl):
    df_azti = pl.read_csv(os.path.join(data_dir, "azti/Short_chain_fatty_acids_in_faeces.tsv"),
                          has_header=True, separator="\t").rename({"ID AZTI": "codkit"}).with_columns(pl.lit("0").alias("timepoint"))
    print(df_azti.head(3))
    return (df_azti,)


@app.cell
def _(df_main):
    df_main
    return


@app.cell
def _(data_dir, df_accexible, df_azti, df_main, df_proteomics, os):
    screening_only_variables = [
        "gds_total_score","sayking_total", "educ_years", "visit_date_screening",
        "caidescotot_v0", "fototest_v_screening", "tam_v_screening", "tot_crq", "educ_level",
    ]

    df_merged = df_main.join(
        df_proteomics, on=["codigo_muestra_plasma_v0", "timepoint"], how="left"
    ).join(
        df_accexible, on=["codvoz", "timepoint"], how="left"
    ).join(
        df_azti, on=["codkit", "timepoint"], how="left"
    )
    vars_to_drop = [c for c in df_merged.columns if c in screening_only_variables]
    df_merged = df_merged.drop(vars_to_drop)
    print(df_merged.head(3))

    df_merged.write_csv(os.path.join(data_dir, "merged_data/cita_and_proteo_and_accexible_and_azti_data_0m_and_12m.csv"))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
