import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import os

    import polars as pl

    return os, pl


@app.cell
def _(os, pl):
    root_dir = os.path.abspath("../")
    data_dir = os.path.join(root_dir, "data/merged_data")

    columns_to_read = ["record_id_SVH", "base_age", "sindr_dx",
                       "demo_gender", "random_group"]

    clinical_data = pl.read_csv(os.path.join(data_dir,
                                            "cita_and_proteo_and_accexible_and_azti_data_0m_and_12m.csv"),
                                has_header=True, columns=columns_to_read, infer_schema=False)

    # Clean
    clinical_data = clinical_data.with_columns(
        pl.col(pl.String).replace("#NULL!", None)
    )
    clinical_data = clinical_data.rename({"base_age": "age"}).with_columns(
        # Non-intervened and cognitively unimpaired
        pl.when((pl.col("random_group") == "0") & (pl.col("sindr_dx") == "1"))
        .then(pl.lit("CN_NonIntervened"))

        # Intervened and cognitively unimpaired
        .when((pl.col("random_group") == "1") & (pl.col("sindr_dx") == "1"))
        .then(pl.lit("CN_Intervened"))

        # Non-intervened and cognitively impaired
        .when((pl.col("random_group") == "0") & (pl.col("sindr_dx").is_in(["2", "3", "4"])))
        .then(pl.lit("CI_NonIntervened"))

        # Intervened and cognitively unimpaired
        .when((pl.col("random_group") == "1") & (pl.col("sindr_dx").is_in(["2", "3", "4"])))
        .then(pl.lit("CI_Intervened"))

        .alias("random_group_label")
    )

    print(clinical_data.head(3))
    return clinical_data, root_dir


@app.cell
def _(clinical_data, os, pl):
    ageml_data_dir = os.path.abspath("../data_for_ageml")
    df_intervention = clinical_data.select(["record_id_SVH", "random_group_label"]).with_columns(
        pl.col("record_id_SVH").cast(pl.Int64),
    ).unique(subset=["record_id_SVH"])
    return ageml_data_dir, df_intervention


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Cardio Features
    """)
    return


@app.cell
def _(ageml_data_dir, df_intervention, os, pl, root_dir):
    t_0_cardio_feats = pl.read_csv(os.path.join(ageml_data_dir, "cardio/cardio_data_with_record_id_SVH_0m.csv"),
                                   has_header=True, infer_schema=False).rename({"": "index"}).with_columns(
                                    pl.col("record_id_SVH").cast(pl.Float32).cast(pl.Int64)
                                   ).with_columns(
                                    pl.lit(0).alias("timepoint")
                                   ).drop("index")
    t_12_cardio_feats = pl.read_csv(os.path.join(ageml_data_dir, "cardio/cardio_data_with_record_id_SVH_12m.csv"),
                                   has_header=True, infer_schema=False).rename({"": "index"}).with_columns(
                                    pl.col("record_id_SVH").cast(pl.Float32).cast(pl.Int64)
                                   ).with_columns(
                                    pl.lit(12).alias("timepoint")
                                   ).drop("index")
    t_all_cardio_feats = pl.concat([t_0_cardio_feats, t_12_cardio_feats], how="vertical")

    t_all_cardio_feats = t_all_cardio_feats.join(
        df_intervention,
        on=["record_id_SVH"],
        how="inner"
    )
    print(t_all_cardio_feats.head(3))

    clinical_series_cardio = t_all_cardio_feats.with_columns(
        pl.when(pl.col("timepoint") == 0)
        .then(pl.lit("CN"))
        .otherwise(pl.when(pl.col("random_group_label") == "CN_Intervened")
                  .then(pl.lit("CN_Intervened"))
                  .otherwise(pl.lit("CN_NonIntervened")))
        .alias("clinical_group")
    ).select("clinical_group")

    clinical_file_cardio = clinical_series_cardio.with_columns([
                    pl.when(pl.col("clinical_group") == "CN")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN"),
                    pl.when(pl.col("clinical_group") == "CN_Intervened")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN_Intervened"),
                    pl.when(pl.col("clinical_group") == "CN_NonIntervened")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN_NonIntervened")
                    ]).drop("clinical_group")
    clinical_file_cardio.head(3)
    assert all(clinical_file_cardio.to_numpy().sum(axis=1))

    save_path_cardio = os.path.join(root_dir, "data_for_ageml/longitudinal_data",
                             "cardio_longitudinal")
    os.makedirs(save_path_cardio, exist_ok=True)

    to_drop_cardio = ["timepoint", "random_group_label"]

    # Save features with and without record_id_SVH
    t_all_cardio_feats.drop(to_drop_cardio).to_pandas().to_csv(os.path.join(save_path_cardio, "longitudinal_cardio_features_with_record_id_SVH.csv"),
                                                        index=True)
    t_all_cardio_feats.drop(to_drop_cardio + ["record_id_SVH"]).to_pandas().to_csv(os.path.join(save_path_cardio, "longitudinal_cardio_features.csv"),
                                                                       index=True)

    # Save the clinical file
    clinical_file_cardio.to_pandas().to_csv(os.path.join(save_path_cardio, "longitudinal_clinical_file.csv"))

    assert clinical_file_cardio.shape[0] == t_all_cardio_feats.shape[0]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Osteomuscular
    """)
    return


@app.cell
def _(ageml_data_dir, df_intervention, os, pl, root_dir):
    t_0_osteomuscular_feats = pl.read_csv(os.path.join(ageml_data_dir, "osteomuscular/osteomuscular_data_with_record_id_SVH_0m.csv"),
                                   has_header=True, infer_schema=False).rename({"": "index"}).with_columns(
                                    pl.col("record_id_SVH").cast(pl.Float32).cast(pl.Int64)
                                   ).with_columns(
                                    pl.lit(0).alias("timepoint")
                                   ).drop("index")
    t_12_osteomuscular_feats = pl.read_csv(os.path.join(ageml_data_dir, "osteomuscular/osteomuscular_data_with_record_id_SVH_12m.csv"),
                                   has_header=True, infer_schema=False).rename({"": "index"}).with_columns(
                                    pl.col("record_id_SVH").cast(pl.Float32).cast(pl.Int64)
                                   ).with_columns(
                                    pl.lit(12).alias("timepoint")
                                   ).drop("index")
    t_all_osteomuscular_feats = pl.concat([t_0_osteomuscular_feats, t_12_osteomuscular_feats], how="vertical")

    t_all_osteomuscular_feats = t_all_osteomuscular_feats.join(
        df_intervention,
        on=["record_id_SVH"],
        how="inner"
    )
    print(t_all_osteomuscular_feats.head(3))

    clinical_series_osteomuscular = t_all_osteomuscular_feats.with_columns(
        pl.when(pl.col("timepoint") == 0)
        .then(pl.lit("CN"))
        .otherwise(pl.when(pl.col("random_group_label") == "CN_Intervened")
                  .then(pl.lit("CN_Intervened"))
                  .otherwise(pl.lit("CN_NonIntervened")))
        .alias("clinical_group")
    ).select("clinical_group")

    clinical_file_osteomuscular = clinical_series_osteomuscular.with_columns([
                    pl.when(pl.col("clinical_group") == "CN")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN"),
                    pl.when(pl.col("clinical_group") == "CN_Intervened")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN_Intervened"),
                    pl.when(pl.col("clinical_group") == "CN_NonIntervened")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN_NonIntervened")
                    ]).drop("clinical_group")
    clinical_file_osteomuscular.head(3)
    assert all(clinical_file_osteomuscular.to_numpy().sum(axis=1))

    save_path_osteomuscular = os.path.join(root_dir, "data_for_ageml/longitudinal_data",
                             "osteomuscular_longitudinal")
    os.makedirs(save_path_osteomuscular, exist_ok=True)

    to_drop_osteomuscular = ["timepoint", "random_group_label"]

    # Save features with and without record_id_SVH
    t_all_osteomuscular_feats.drop(to_drop_osteomuscular).to_pandas().to_csv(os.path.join(save_path_osteomuscular, "longitudinal_osteomuscular_features_with_record_id_SVH.csv"),
                                                        index=True)
    t_all_osteomuscular_feats.drop(to_drop_osteomuscular + ["record_id_SVH"]).to_pandas().to_csv(os.path.join(save_path_osteomuscular, "longitudinal_osteomuscular_features.csv"),
                                                                       index=True)

    # Save the clinical file
    clinical_file_osteomuscular.to_pandas().to_csv(os.path.join(save_path_osteomuscular, "longitudinal_clinical_file.csv"))

    assert clinical_file_osteomuscular.shape[0] == t_all_osteomuscular_feats.shape[0]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Cerebral
    """)
    return


@app.cell
def _(ageml_data_dir, df_intervention, os, pl, root_dir):
    t_0_cerebral_feats = pl.read_csv(os.path.join(ageml_data_dir, "cerebral/cerebral_data_with_record_id_SVH_0m.csv"),
                                   has_header=True, infer_schema=False).rename({"": "index"}).with_columns(
                                    pl.col("record_id_SVH").cast(pl.Float32).cast(pl.Int64)
                                   ).with_columns(
                                    pl.lit(0).alias("timepoint")
                                   ).drop("index")
    t_12_cerebral_feats = pl.read_csv(os.path.join(ageml_data_dir, "cerebral/cerebral_data_with_record_id_SVH_12m.csv"),
                                   has_header=True, infer_schema=False).rename({"": "index"}).with_columns(
                                    pl.col("record_id_SVH").cast(pl.Float32).cast(pl.Int64)
                                   ).with_columns(
                                    pl.lit(12).alias("timepoint")
                                   ).drop("index")
    t_all_cerebral_feats = pl.concat([t_0_cerebral_feats, t_12_cerebral_feats], how="vertical")

    t_all_cerebral_feats = t_all_cerebral_feats.join(
        df_intervention,
        on=["record_id_SVH"],
        how="inner"
    )
    print(t_all_cerebral_feats.head(3))

    clinical_series_cerebral = t_all_cerebral_feats.with_columns(
        pl.when(pl.col("timepoint") == 0)
        .then(pl.lit("CN"))
        .otherwise(pl.when(pl.col("random_group_label") == "CN_Intervened")
                  .then(pl.lit("CN_Intervened"))
                  .otherwise(pl.lit("CN_NonIntervened")))
        .alias("clinical_group")
    ).select("clinical_group")

    clinical_file_cerebral = clinical_series_cerebral.with_columns([
                    pl.when(pl.col("clinical_group") == "CN")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN"),
                    pl.when(pl.col("clinical_group") == "CN_Intervened")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN_Intervened"),
                    pl.when(pl.col("clinical_group") == "CN_NonIntervened")
                    .then(pl.lit(1))
                    .otherwise(pl.lit(0))
                    .alias("CN_NonIntervened")
                    ]).drop("clinical_group")
    clinical_file_cerebral.head(3)
    assert all(clinical_file_cerebral.to_numpy().sum(axis=1))

    save_path_cerebral = os.path.join(root_dir, "data_for_ageml/longitudinal_data",
                             "cerebral_longitudinal")
    os.makedirs(save_path_cerebral, exist_ok=True)

    to_drop_cerebral = ["timepoint", "random_group_label"]

    # Save features with and without record_id_SVH
    t_all_cerebral_feats.drop(to_drop_cerebral).to_pandas().to_csv(os.path.join(save_path_cerebral, "longitudinal_cerebral_features_with_record_id_SVH.csv"),
                                                        index=True)
    t_all_cerebral_feats.drop(to_drop_cerebral + ["record_id_SVH"]).to_pandas().to_csv(os.path.join(save_path_cerebral, "longitudinal_cerebral_features.csv"),
                                                                       index=True)

    # Save the clinical file
    clinical_file_cerebral.to_pandas().to_csv(os.path.join(save_path_cerebral, "longitudinal_clinical_file.csv"))

    assert clinical_file_cerebral.shape[0] == t_all_cerebral_feats.shape[0]
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
