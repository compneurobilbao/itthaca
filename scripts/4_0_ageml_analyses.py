import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import os

    import subprocess as sp

    return os, sp


@app.cell
def _():
    act_venv = "source /home/teitxe/venvs/neurovenv/bin/activate"
    return (act_venv,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # CROSS SECTIONAL MODELING
    ## t=0m
    """)
    return


@app.cell
def _(os):
    cross_sec_analyses_dir = os.path.abspath("../analyses/cross_sectional")
    os.makedirs(cross_sec_analyses_dir, exist_ok=True)

    data_dir = os.path.abspath("../data_for_ageml")
    return cross_sec_analyses_dir, data_dir


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Cardio
    """)
    return


@app.cell
def _(act_venv, cross_sec_analyses_dir, data_dir, os, sp):
    cross_sec_cardio_features_fname = os.path.join(data_dir, "cardio", "cardio_data_0m.csv")
    cross_sec_cardio_output_dir = os.path.join(cross_sec_analyses_dir, "cardio_cross_sectional_0m")
    cross_sec_cardio_clinical_fname = os.path.join(data_dir, "cardio", "cardio_clinical_0m.csv")

    cross_sec_cardio_call = (f"{act_venv} && model_age -o {cross_sec_cardio_output_dir} "
                             f"-f {cross_sec_cardio_features_fname} --clinical {cross_sec_cardio_clinical_fname}")
    print(f"Running cross-sectional cardio analysis...Command:\n{cross_sec_cardio_call}")
    sp.run(cross_sec_cardio_call, shell=True, capture_output=True, text=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Cerebral
    """)
    return


@app.cell
def _(act_venv, cross_sec_analyses_dir, data_dir, os, sp):
    cross_sec_cerebral_features_fname = os.path.join(data_dir, "cerebral", "cerebral_data_0m.csv")
    cross_sec_cerebral_output_dir = os.path.join(cross_sec_analyses_dir, "cerebral_cross_sectional_0m")
    cross_sec_cerebral_clinical_fname = os.path.join(data_dir, "cerebral", "cerebral_clinical_0m.csv")

    cross_sec_cerebral_call = (f"{act_venv} && model_age -o {cross_sec_cerebral_output_dir} "
                             f"-f {cross_sec_cerebral_features_fname} --clinical {cross_sec_cerebral_clinical_fname}")
    print(f"Running cross-sectional cerebral analysis...Command:\n{cross_sec_cerebral_call}")
    sp.run(cross_sec_cerebral_call, shell=True, capture_output=True, text=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Osteomuscular
    """)
    return


@app.cell
def _(act_venv, cross_sec_analyses_dir, data_dir, os, sp):
    cross_sec_osteomuscular_features_fname = os.path.join(data_dir, "osteomuscular", "osteomuscular_data_0m.csv")
    cross_sec_osteomuscular_output_dir = os.path.join(cross_sec_analyses_dir, "osteomuscular_cross_sectional_0m")
    cross_sec_osteomuscular_clinical_fname = os.path.join(data_dir, "osteomuscular", "osteomuscular_clinical_0m.csv")

    cross_sec_osteomuscular_call = (f"{act_venv} && model_age -o {cross_sec_osteomuscular_output_dir} "
                             f"-f {cross_sec_osteomuscular_features_fname} --clinical {cross_sec_osteomuscular_clinical_fname}")
    print(f"Running cross-sectional osteomuscular analysis...Command:\n{cross_sec_osteomuscular_call}")
    sp.run(cross_sec_osteomuscular_call, shell=True, capture_output=True, text=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Azti
    """)
    return


@app.cell
def _(act_venv, cross_sec_analyses_dir, data_dir, os, sp):
    cross_sec_azti_features_fname = os.path.join(data_dir, "azti", "azti_data_0m.csv")
    cross_sec_azti_output_dir = os.path.join(cross_sec_analyses_dir, "azti_cross_sectional_0m")
    cross_sec_azti_clinical_fname = os.path.join(data_dir, "azti", "azti_clinical_0m.csv")

    cross_sec_azti_call = (f"{act_venv} && model_age -o {cross_sec_azti_output_dir} "
                             f"-f {cross_sec_azti_features_fname} --clinical {cross_sec_azti_clinical_fname}")
    print(f"Running cross-sectional azti analysis...Command:\n{cross_sec_azti_call}")
    sp.run(cross_sec_azti_call, shell=True, capture_output=True, text=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # LONGITUDINAL MODELING
    """)
    return


@app.cell
def _(os):
    longitudinal_analyses_dir = os.path.abspath("../analyses/longitudinal")
    os.makedirs(longitudinal_analyses_dir, exist_ok=True)
    return (longitudinal_analyses_dir,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Cardio
    """)
    return


@app.cell
def _(act_venv, data_dir, longitudinal_analyses_dir, os, sp):
    longitudinal_cardio_features_fname = os.path.join(data_dir, "longitudinal_data/cardio_longitudinal/longitudinal_cardio_features.csv")
    longitudinal_cardio_output_dir = os.path.join(longitudinal_analyses_dir, "cardio_longitudinal")
    longitudinal_cardio_clinical_fname = os.path.join(data_dir, "longitudinal_data/cardio_longitudinal/longitudinal_clinical_file.csv")

    longitudinal_cardio_call = (f"{act_venv} && model_age -o {longitudinal_cardio_output_dir} "
                             f"-f {longitudinal_cardio_features_fname} --clinical {longitudinal_cardio_clinical_fname}")
    print(f"Running longitudinal cardio analysis...Command:\n{longitudinal_cardio_call}")
    sp.run(longitudinal_cardio_call, shell=True, capture_output=True, text=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Cerebral
    """)
    return


@app.cell
def _(act_venv, data_dir, longitudinal_analyses_dir, os, sp):
    longitudinal_cerebral_features_fname = os.path.join(data_dir, "longitudinal_data/cerebral_longitudinal/longitudinal_cerebral_features.csv")
    longitudinal_cerebral_output_dir = os.path.join(longitudinal_analyses_dir, "cerebral_longitudinal")
    longitudinal_cerebral_clinical_fname = os.path.join(data_dir, "longitudinal_data/cerebral_longitudinal/longitudinal_clinical_file.csv")

    longitudinal_cerebral_call = (f"{act_venv} && model_age -o {longitudinal_cerebral_output_dir} "
                             f"-f {longitudinal_cerebral_features_fname} --clinical {longitudinal_cerebral_clinical_fname}")
    print(f"Running longitudinal cerebral analysis...Command:\n{longitudinal_cerebral_call}")
    sp.run(longitudinal_cerebral_call, shell=True, capture_output=True, text=True)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Osteomuscular
    """)
    return


@app.cell
def _(act_venv, data_dir, longitudinal_analyses_dir, os, sp):
    longitudinal_osteomuscular_features_fname = os.path.join(data_dir, "longitudinal_data/osteomuscular_longitudinal/longitudinal_osteomuscular_features.csv")
    longitudinal_osteomuscular_output_dir = os.path.join(longitudinal_analyses_dir, "osteomuscular_longitudinal")
    longitudinal_osteomuscular_clinical_fname = os.path.join(data_dir, "longitudinal_data/osteomuscular_longitudinal/longitudinal_clinical_file.csv")

    longitudinal_osteomuscular_call = (f"{act_venv} && model_age -o {longitudinal_osteomuscular_output_dir} "
                             f"-f {longitudinal_osteomuscular_features_fname} --clinical {longitudinal_osteomuscular_clinical_fname}")
    print(f"Running longitudinal osteomuscular analysis...Command:\n{longitudinal_osteomuscular_call}")
    sp.run(longitudinal_osteomuscular_call, shell=True, capture_output=True, text=True)
    return


if __name__ == "__main__":
    app.run()
