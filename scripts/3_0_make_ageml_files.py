import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import os

    import polars as pl

    return os, pl


@app.cell
def _(pl):
    df = pl.read_csv("../data/merged_data/cita_and_proteo_and_accexible_and_azti_data_0m_and_12m.csv",
                     has_header=True, infer_schema=False).rename({"saykin_total": "sayking_total"})

    df.head(3)
    return (df,)


@app.cell
def _():
    cardio_vars = ["imc_enf", "cint_enf", "cad_enf",
                   "pas_enf", "pad_enf",
                   "glucose",
                #    "colesterol_total", "npacks_years", "ubes_week", "hta", "dlp", "dm"
                   ]

    osteomuscular_vars = [
        # "total_marcha_tinetti",
        "sppb_mar_velo", "m_normal_velo", "handgrip",
        "total_scopa", "total_equilib_tinetti",
        "total_tinetti"
    ]

    cerebral_vars = [ # Cognitive, depressive, etc. Included pittsburgh because it is a proxy of depressive state.
        # "gds_total_score", "sayking_total", "caidescotot_v0", "cst_alternanceindex", "tot_crq"
        "num_psicofarmacos",
        "anxtotdef", "deprestotdef", "indx_psq",
        "healthstate_eq5d", "tot_substtion",
        "tme_tmta", "tot_digit", "tot_letnum_wais",
        "tme_tmtb", "tmewordc_strp40",
        "recall_cerad", "dly_logmry", "sem_vf",
        "calidad_subjetiva_sueno"
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

    common_vars = ["base_age", "sindr_dx", "record_id_SVH", "timepoint"]
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
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999") | (pl.col(pl.String) == "999") | (pl.col(pl.String) == "999.0"))
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Replace all the "#NULL!" with null values
        pl.when(pl.col(pl.String) == "#NULL!")
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast record_id_SVH to string
        pl.col("record_id_SVH").cast(pl.String).name.keep()
    ).rename({"base_age": "age"}
    ).with_columns(
        age = pl.when(pl.col("timepoint") == 12)
        .then(pl.col("age").max().over("record_id_SVH") + 1)
        .otherwise(pl.col("age"))
    )

    df_cardio_0m = df_cardio.filter(pl.col("timepoint") == 0).drop("timepoint")

    # Get the clinical file
    df_cardio_clinical = (
        df_cardio.select(["sindr_dx", "timepoint"])
        .with_columns(
            sindr_dx = pl.when(pl.col("sindr_dx") == 1).then(pl.lit("CN")).otherwise(pl.lit("CASE"))
        )
        .to_dummies("sindr_dx")
        .rename({"sindr_dx_CASE": "CASE", "sindr_dx_CN": "CN"})
        .with_columns(
            # This converts 1/0 to true/false boolean, then to "true"/"false" string
            pl.col("CASE", "CN").cast(pl.Boolean).cast(pl.String).str.to_titlecase()
        )
    )

    df_cardio_clinical_0m = df_cardio_clinical.filter(pl.col("timepoint") == 0)

    # Save the files
    os.makedirs("../data_for_ageml/cardio", exist_ok=True)
    df_cardio_0m.drop(["sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cardio/cardio_data_with_record_id_SVH_0m.csv", index=True)
    df_cardio_0m.drop(["record_id_SVH", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cardio/cardio_data_0m.csv", index=True)
    df_cardio_clinical_0m.drop("timepoint").to_pandas().to_csv("../data_for_ageml/cardio/cardio_clinical_0m.csv", index=True)


    # Now do the same but for the tiempoint=="12m"
    df_cardio_12m = df_cardio.filter(pl.col("timepoint") == 12).drop("timepoint")
    df_cardio_12m.drop(["sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cardio/cardio_data_with_record_id_SVH_12m.csv", index=True)
    df_cardio_12m.drop(["record_id_SVH", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cardio/cardio_data_12m.csv", index=True)
    df_cardio_clinical_12m = df_cardio_clinical.filter(pl.col("timepoint") == 12)
    df_cardio_clinical_12m.drop("timepoint").to_pandas().to_csv("../data_for_ageml/cardio/cardio_clinical_12m.csv", index=True)
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
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999") | (pl.col(pl.String) == "999") | (pl.col(pl.String) == "999.0"))
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Replace all the "#NULL!" with null values
        pl.when(pl.col(pl.String) == "#NULL!")
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast record_id_SVH to string
        pl.col("record_id_SVH").cast(pl.String).name.keep()
    ).rename({"base_age": "age"}
    ).with_columns(
        age = pl.when(pl.col("timepoint") == 12)
        .then(pl.col("age").max().over("record_id_SVH") + 1)
        .otherwise(pl.col("age"))
    )

    df_osteomuscular_0m = df_osteomuscular.filter(pl.col("timepoint") == 0).drop("timepoint")

    # Get the clinical file
    df_osteomuscular_clinical = (
        df_osteomuscular.select(["sindr_dx", "timepoint"])
        .with_columns(
            sindr_dx = pl.when(pl.col("sindr_dx") == 1).then(pl.lit("CN")).otherwise(pl.lit("CASE"))
        )
        .to_dummies("sindr_dx")
        .rename({"sindr_dx_CASE": "CASE", "sindr_dx_CN": "CN"})
        .with_columns(
            # This converts 1/0 to true/false boolean, then to "true"/"false" string
            pl.col("CASE", "CN").cast(pl.Boolean).cast(pl.String).str.to_titlecase()
        )
    )

    df_osteomuscular_clinical_0m = df_osteomuscular_clinical.filter(pl.col("timepoint") == 0)

    # Save the files
    os.makedirs("../data_for_ageml/osteomuscular", exist_ok=True)
    df_osteomuscular_0m.drop(["sindr_dx"]).to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_data_with_record_id_SVH_0m.csv", index=True)
    df_osteomuscular_0m.drop(["record_id_SVH", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_data_0m.csv", index=True)
    df_osteomuscular_clinical_0m.drop("timepoint").to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_clinical_0m.csv", index=True)


    # Now do the same but for the tiempoint=="12m"
    df_osteomuscular_12m = df_osteomuscular.filter(pl.col("timepoint") == 12).drop("timepoint")
    df_osteomuscular_12m.drop(["sindr_dx"]).to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_data_with_record_id_SVH_12m.csv", index=True)
    df_osteomuscular_12m.drop(["record_id_SVH", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_data_12m.csv", index=True)
    df_osteomuscular_clinical_12m = df_osteomuscular_clinical.filter(pl.col("timepoint") == 12)
    df_osteomuscular_clinical_12m.drop("timepoint").to_pandas().to_csv("../data_for_ageml/osteomuscular/osteomuscular_clinical_12m.csv", index=True)
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
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999") | (pl.col(pl.String) == "999") | (pl.col(pl.String) == "999.0"))
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Replace all the "#NULL!" with null values
        pl.when(pl.col(pl.String) == "#NULL!")
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast record_id_SVH to string
        pl.col("record_id_SVH").cast(pl.String).name.keep()
    ).rename({"base_age": "age"}
    ).with_columns(
        age = pl.when(pl.col("timepoint") == 12)
        .then(pl.col("age").max().over("record_id_SVH") + 1)
        .otherwise(pl.col("age"))
    )

    df_cerebral_0m = df_cerebral.filter(pl.col("timepoint") == 0).drop("timepoint")

    # Get the clinical file
    df_cerebral_clinical = (
        df_cerebral.select(["sindr_dx", "timepoint"])
        .with_columns(
            sindr_dx = pl.when(pl.col("sindr_dx") == 1).then(pl.lit("CN")).otherwise(pl.lit("CASE"))
        )
        .to_dummies("sindr_dx")
        .rename({"sindr_dx_CASE": "CASE", "sindr_dx_CN": "CN"})
        .with_columns(
            # This converts 1/0 to true/false boolean, then to "true"/"false" string
            pl.col("CASE", "CN").cast(pl.Boolean).cast(pl.String).str.to_titlecase()
        )
    )

    df_cerebral_clinical_0m = df_cerebral_clinical.filter(pl.col("timepoint") == 0)

    # Save the files
    os.makedirs("../data_for_ageml/cerebral", exist_ok=True)
    df_cerebral_0m.drop(["sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_data_with_record_id_SVH_0m.csv", index=True)
    df_cerebral_0m.drop(["record_id_SVH", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_data_0m.csv", index=True)
    df_cerebral_clinical_0m.drop("timepoint").to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_clinical_0m.csv", index=True)


    # Now do the same but for the tiempoint=="12m"
    df_cerebral_12m = df_cerebral.filter(pl.col("timepoint") == 12).drop("timepoint")
    df_cerebral_12m.drop(["sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_data_with_record_id_SVH_12m.csv", index=True)
    df_cerebral_12m.drop(["record_id_SVH", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_data_12m.csv", index=True)
    df_cerebral_clinical_12m = df_cerebral_clinical.filter(pl.col("timepoint") == 12)
    df_cerebral_clinical_12m.drop("timepoint").to_pandas().to_csv("../data_for_ageml/cerebral/cerebral_clinical_12m.csv", index=True)
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
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999") | (pl.col(pl.String) == "999") | (pl.col(pl.String) == "999.0"))
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Replace all the "#NULL!" with null values
        pl.when(pl.col(pl.String) == "#NULL!")
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    ).with_columns(
        # Now cast all columns to float
        pl.col(pl.String).cast(pl.Float64).name.keep()
    ).with_columns(
        # Finally, cast record_id_SVH to string
        pl.col("record_id_SVH").cast(pl.String).name.keep()
    ).rename({"base_age": "age"}).filter(pl.col("timepoint") == 0).drop("timepoint")

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

    # Save with the subject id for future merging
    df_azti.drop(["sindr_dx"]).to_pandas().to_csv("../data_for_ageml/azti/azti_data_with_record_id_SVH_0m.csv", index=True)
    df_azti.drop(["record_id_SVH", "sindr_dx"]).to_pandas().to_csv("../data_for_ageml/azti/azti_data_0m.csv", index=True)
    df_azti_clinical.to_pandas().to_csv("../data_for_ageml/azti/azti_clinical_0m.csv", index=True)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
