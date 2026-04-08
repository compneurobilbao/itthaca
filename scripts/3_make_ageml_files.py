import marimo

__generated_with = "0.19.11"
app = marimo.App()


@app.cell
def _():
    import os

    import polars as pl

    return os, pl


@app.cell
def _(pl):
    df = pl.read_csv("../data/merged_data/cita_and_proteo_and_accexible_data.csv",
                     has_header=True, infer_schema=False).rename({"saykin_total": "sayking_total"})
    df.head(5)
    return (df,)


@app.cell
def _():
    cardio_vars = ["imc_enf", "cint_enf", "cad_enf",
                   "pas_enf", "pad_enf", "colesterol_total",
                   "glucose", "npacks_years",
                   "ubes_week", "hta", "dlp", "dm"]

    osteomuscular_vars = [
        "sppb_mar_velo", "m_normal_velo", "handgrip",
        "total_scopa", "total_equilib_tinetti", "total_marcha_tinetti",
        "total_tinetti"
    ]

    cerebral_vars = [ # Cognitive, depressive, etc. Included pittsburgh because it is a proxy of depressive state.
        "gds_total_score", "num_psicofarmacos", "sayking_total",
        "anxtotdef", "deprestotdef", "indx_psq",
        "healthstate_eq5d", "caidescotot_v0", "tot_substtion_vbasal",
        "tme_tmta_vbasal", "tot_digit_vbasal", "tot_letnum_wais_vbasal",
        "tme_tmtb_vbasal", "tmewordc_strp40_vbasal", "cst_alternanceindex_vbasal",
        "recall_cerad_vbasal", "dly_logmry_vbasal", "sem_vf_vbasal",
        "tot_crq", "calidad_subjetiva_sueno"
    ]

    azti_vars = [
        "Acetato_3_SCFA",
        "Propionato_3_SCFA",
        "Butirato_3_SCFA",
        "Acetato_8_SCFA",
        "Propionato_8_SCFA",
        "Butirato_8_SCFA",
        "Isobutirato_8_SCFA",
        "Valerato_8_SCFA",
        "Isovalerato_8_SCFA",
        "Hexanoato_8_SCFA",
        "Heptanoato_8_SCFA",
        "Major",
        "Minor",
        "Major/Minor",
        "Propionato/Butírato",
        "Propionato-Butírato"
    ]

    common_vars = ["base_age", "sindr_dx", "codcita"]
    return (
        azti_vars,
        cardio_vars,
        cerebral_vars,
        common_vars,
        osteomuscular_vars,
    )


@app.cell
def _(os):
    os.makedirs("../data_for_ageml/", exist_ok=True)
    return


@app.cell
def _(mo):
    mo.md(r"""
    # CARDIO
    """)
    return


@app.cell
def _(cardio_vars, common_vars, df, os, pl):
    df_cardio = df.select(cardio_vars + common_vars).with_columns(
        # Replace all "9999.0" with 0
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999"))
          .then(pl.lit("0"))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast codcita to string
        pl.col("codcita").cast(pl.String).name.keep()
    ).rename({"base_age": "age"})

    # Get the clinical file
    df_cardio_clinical = df_cardio.select("sindr_dx").with_columns(
                            pl.when(pl.col("sindr_dx") == 1)
                            .then(pl.lit("CN"))
                            .otherwise(pl.lit("CASE")).name.keep()
                         ).to_dummies().rename({"sindr_dx_CASE": "CASE", "sindr_dx_CN": "CN"}).with_columns(
                            pl.all().cast(pl.String).name.keep()
                         ).with_columns(
                            pl.when(pl.col(pl.String) == "1")
                            .then(pl.lit("True"))
                            .otherwise(pl.lit("False")).name.keep()
                         )

    # Save the files
    os.makedirs("../data_for_ageml/cardio", exist_ok=True)
    df_cardio.drop(["codcita", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cardio/cardio_data.csv", index=True)
    df_cardio_clinical.to_pandas().to_csv("../data_for_ageml/cardio/cardio_clinical.csv", index=True)
    return


@app.cell
def _(mo):
    mo.md(r"""
    # OSTEOMUSCULAR
    """)
    return


@app.cell
def _(common_vars, df, os, osteomuscular_vars, pl):
    df_osteomuscular = df.select(osteomuscular_vars + common_vars).with_columns(
        # Replace all "9999.0" with 0
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999"))
          .then(pl.lit("0"))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast codcita to string
        pl.col("codcita").cast(pl.String).name.keep()
    ).rename({"base_age": "age"})

    # Get the clinical file
    df_osteomuscular_clinical = df_osteomuscular.select("sindr_dx").with_columns(
                            pl.when(pl.col("sindr_dx") == 1)
                            .then(pl.lit("CN"))
                            .otherwise(pl.lit("CASE")).name.keep()
                         ).to_dummies().rename({"sindr_dx_CASE": "CASE", "sindr_dx_CN": "CN"}).with_columns(
                            pl.all().cast(pl.String).name.keep()
                         ).with_columns(
                            pl.when(pl.col(pl.String) == "1")
                            .then(pl.lit("True"))
                            .otherwise(pl.lit("False")).name.keep()
                         )

    # Save the files
    os.makedirs("../data_for_ageml/osteomuscular", exist_ok=True)
    df_osteomuscular.drop(["codcita", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_data.csv", index=True)
    df_osteomuscular_clinical.to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_clinical.csv", index=True)
    return


@app.cell
def _(mo):
    mo.md(r"""
    # CEREBRAL
    """)
    return


@app.cell
def _(cerebral_vars, common_vars, df, os, pl):
    df_cerebral = df.select(cerebral_vars + common_vars).with_columns(
        # Replace all "9999.0" with 0
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999"))
          .then(pl.lit("0"))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast codcita to string
        pl.col("codcita").cast(pl.String).name.keep()
    ).rename({"base_age": "age"})

    # Get the clinical file
    df_cerebral_clinical = df_cerebral.select("sindr_dx").with_columns(
                            pl.when(pl.col("sindr_dx") == 1)
                            .then(pl.lit("CN"))
                            .otherwise(pl.lit("CASE")).name.keep()
                         ).to_dummies().rename({"sindr_dx_CASE": "CASE", "sindr_dx_CN": "CN"}).with_columns(
                            pl.all().cast(pl.String).name.keep()
                         ).with_columns(
                            pl.when(pl.col(pl.String) == "1")
                            .then(pl.lit("True"))
                            .otherwise(pl.lit("False")).name.keep()
                         )

    # Save the files
    os.makedirs("../data_for_ageml/cerebral", exist_ok=True)
    df_cerebral.drop(["codcita", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_data.csv", index=True)
    df_cerebral_clinical.to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_clinical.csv", index=True)
    return


@app.cell
def _(mo):
    mo.md(r"""
    # AZTI
    """)
    return


@app.cell
def _(azti_vars, common_vars, df, os, pl):
    df_azti = df.select(azti_vars + common_vars).with_columns(
        # Replace all "9999.0" with 0
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999"))
          .then(pl.lit("0"))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast codcita to string
        pl.col("codcita").cast(pl.String).name.keep()
    ).rename({"base_age": "age"})

    # Get the clinical file
    df_azti_clinical = df_azti.select("sindr_dx").with_columns(
                            pl.when(pl.col("sindr_dx") == 1)
                            .then(pl.lit("CN"))
                            .otherwise(pl.lit("CASE")).name.keep()
                         ).to_dummies().rename({"sindr_dx_CASE": "CASE", "sindr_dx_CN": "CN"}).with_columns(
                            pl.all().cast(pl.String).name.keep()
                         ).with_columns(
                            pl.when(pl.col(pl.String) == "1")
                            .then(pl.lit("True"))
                            .otherwise(pl.lit("False")).name.keep()
                         )

    # Save the files
    os.makedirs("../data_for_ageml/azti", exist_ok=True)
    df_azti.drop(["codcita", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/azti/azti_data.csv", index=True)
    df_azti_clinical.to_pandas().to_csv("../data_for_ageml/azti/azti_clinical.csv", index=True)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
