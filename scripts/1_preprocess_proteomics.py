import marimo

__generated_with = "0.19.9"
app = marimo.App()


@app.cell
def _(mo):
    mo.md(r"""
    ### Load libraries
    """)
    return


@app.cell
def _():
    import os
    import polars as pl

    import statsmodels.formula.api as smf

    return os, pl, smf


@app.cell
def _(mo):
    mo.md(r"""
    ### Paths
    """)
    return


@app.cell
def _(os):
    root_dir = os.path.abspath("../")
    data_dir = os.path.join(root_dir, "data")
    return (data_dir,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Preprocess
    """)
    return


@app.cell
def _(data_dir, os, pl):
    proteomics_df = pl.read_excel(
        os.path.join(data_dir, "ZUGAZA_RESULTS9_readable.ods"),
        sheet_name="CONTAR"
    ).filter(
        pl.col("Contar ") > 50
    )
    # .with_columns(
    #     C_65 = pl.lit("NA")
    # )
    return (proteomics_df,)


@app.cell
def _(pl, proteomics_df):
    protein_names = proteomics_df["Protein.Names"].to_list()
    participant_ids = proteomics_df.columns[6:206]

    to_replace = [a for a in protein_names if ";" in a]
    replacements = {name: name.replace(";", "") for name in to_replace}

    df_proteomics = proteomics_df.unpivot(
        index="Protein.Names",
        on=participant_ids,
        variable_name="participant_id",
        value_name="Expression_level"
    ).pivot(
        on="Protein.Names",
        index="participant_id",
        values="Expression_level"
    ).with_columns(
        # If the participant_id starts by "C_", make the group "Control", else "Intervened"
        group = pl.when(pl.col("participant_id").str.starts_with("C_"))
                  .then(0)
                  .otherwise(1)
    ).filter(
        pl.col("participant_id") != "C_65"  # Drop the participant C_65, because it has all NA values.
    ).with_columns(
        # Cast all protein name columns to Float64
        [pl.col(protein_name).cast(pl.Float64) for protein_name in protein_names]
    ).rename(
        # Remove all ; characters in protein names
        replacements
    )

    # Remove the "_" and the "1433" substrings from the column names
    renamed_columns = {col: col.replace("_", "").replace("1433", "Y") for col in df_proteomics.columns[1:-1]}
    df_proteomics = df_proteomics.rename(renamed_columns)

    protein_names = df_proteomics.columns[1:-1]  # Exclude participant_id column
    return df_proteomics, protein_names


@app.cell
def _(mo):
    mo.md(r"""
    ### Statistical testing on the protein values
    """)
    return


@app.cell
def _(df_proteomics, protein_names, smf):
    significant_proteins = []
    p_values = {}
    for protein_name in protein_names:
            p_value = smf.glm(f"group ~ {protein_name}", data=df_proteomics.to_pandas()).fit().pvalues[protein_name]
            p_values[protein_name] = p_value
            if p_value < 0.05 / len(protein_names):  # Bonferroni correction
                print(f"Protein {protein_name} is significant with p-value {p_value}")
                significant_proteins.append(protein_name)
    return (p_values,)


@app.cell
def _(p_values):
    # FDR correction for multiple testing
    from statsmodels.stats.multitest import multipletests
    pvals = list(p_values.values())
    reject, pvals_corrected, _, _ = multipletests(pvals, alpha=0.05, method='fdr_bh')
    significant_proteins_fdr = [protein for protein, rej in zip(p_values.keys(), reject) if rej]
    print(f"Significant proteins after FDR correction: {significant_proteins_fdr}")
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Save the new dataframe
    """)
    return


@app.cell
def _():
    # df_proteomics.write_csv(os.path.join(data_dir, "proteomics_preprocessed.csv"), separator=",", include_header=True)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
