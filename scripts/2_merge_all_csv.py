import marimo

__generated_with = "0.19.11"
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
    df_main = pl.read_excel(os.path.join(data_dir,
                            "ITT_SHV_CITA-ALZHEIMER_BD-BASAL_sindr_dx.xls"))
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
    )
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
                               )
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
                          has_header=True, separator="\t").rename({"ID AZTI": "codkit"})
    return (df_azti,)


@app.cell
def _(data_dir, df_accexible, df_azti, df_main, df_proteomics, os):
    df_merged = df_main.join(
        df_proteomics, on="codigo_muestra_plasma_v0", how="left"
    ).join(
        df_accexible, on="codvoz", how="left"
    ).join(
        df_azti, on="codkit", how="left"
    )

    df_merged.write_csv(os.path.join(data_dir, "merged_data/cita_and_proteo_and_accexible_and_azti_data.csv"))
    return


if __name__ == "__main__":
    app.run()
