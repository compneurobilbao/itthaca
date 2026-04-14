import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import os
    import polars as pl
    import statsmodels.formula.api as smf
    import statsmodels.api as sm
    import scipy.stats as stats

    return os, pl, stats


@app.cell
def _(os, pl):
    data_path = os.path.abspath("../data/ITT_SHV_CITA-ALZHEIMER_BD-BASAL_sindr_dx.xls")

    df = pl.read_excel(data_path)
    df_demographics = df.select([
        "sindr_dx", "random_group", "base_age", "demo_gender", "educ_years"
    ])
    print(df_demographics.head(3))
    return df, df_demographics


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Age
    """)
    return


@app.cell
def _(df_demographics, pl, stats):
    stats.ttest_ind(
        df_demographics.filter(pl.col("random_group") == 1).select("base_age").to_numpy().flatten(),
        df_demographics.filter(pl.col("random_group") == 0).select("base_age").to_numpy().flatten()
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Sex
    """)
    return


@app.cell
def _(df, stats):
    ct = (
        df.group_by(["random_group", "demo_gender"])
        .len()
        .pivot(on="demo_gender", index="random_group", values="len")
        .fill_null(0)
    ).drop("random_group").to_numpy()

    stats.chi2_contingency(ct)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Education Years
    """)
    return


@app.cell
def _(df_demographics, pl, stats):
    stats.ttest_ind(
        df_demographics.filter(pl.col("random_group") == 1).select("educ_years").to_numpy().flatten(),
        df_demographics.filter(pl.col("random_group") == 0).select("educ_years").to_numpy().flatten()
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Table
    """)
    return


@app.cell
def _(df_demographics, pl):
    print(df_demographics.filter(pl.col("random_group") == 1)["demo_gender"].value_counts())
    print(df_demographics.filter(pl.col("random_group") == 0)["demo_gender"].value_counts())
    return


@app.cell
def _(df_demographics, pl):
    print(df_demographics.filter(pl.col("random_group") == 0).select("base_age").describe())
    print(df_demographics.filter(pl.col("random_group") == 1).select("base_age").describe())
    return


@app.cell
def _(df_demographics, pl):
    print(df_demographics.filter(pl.col("random_group") == 0).select("educ_years").describe())
    print(df_demographics.filter(pl.col("random_group") == 1).select("educ_years").describe())
    return


if __name__ == "__main__":
    app.run()
