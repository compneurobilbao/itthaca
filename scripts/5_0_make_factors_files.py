import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import os

    import polars as pl

    import statsmodels.api as sm
    import statsmodels.formula.api as smf

    return os, pl, sm, smf


@app.cell
def _(os):
    root_dir = os.path.abspath("../")
    data_dir = os.path.join(root_dir, "data/merged_data")
    return (data_dir,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
 
    """)
    return


@app.cell
def _(data_dir, os, pl):
    clinical_data = pl.read_csv(os.path.join(data_dir,
                                            "cita_and_proteo_and_accexible_and_azti_data_0m_and_12m.csv"),
                                has_header=True, infer_schema=False)

    clinical_data = clinical_data.with_columns(
        pl.col(pl.String).replace("#NULL!", None)
    ).with_columns(
        # Replace all "9999.0" with 0
        pl.when((pl.col(pl.String) == "9999.0") | (pl.col(pl.String) == "9999") | (pl.col(pl.String) == "999") | (pl.col(pl.String) == "999.0"))
          .then(pl.lit(None))
          .otherwise(pl.col(pl.String)).name.keep()
    )
    clinical_data = clinical_data.rename({"base_age": "age"})
    print(clinical_data.head(3))
    print(clinical_data.shape)
    return (clinical_data,)


@app.cell
def _(clinical_data):
    clinical_data["educ_years"]
    return


@app.cell
def _(clinical_data, sm, smf):
    smf.glm("random_group ~ educ_years",
            data=clinical_data,
            family=sm.families.Binomial()).fit().summary()
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
