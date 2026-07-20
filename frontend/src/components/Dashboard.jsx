function Dashboard({ summary }) {

  if (!summary) {

    return (

      <section className="panel quality-panel">

        <div className="panel-header">

          <div>

            <h2>
              Data Quality
            </h2>

            <p>
              Latest validation results.
            </p>

          </div>

        </div>


        <div className="quality-loading">

          Loading quality results...

        </div>

      </section>

    );

  }


  const overall =
    summary.overall;


  return (

    <section className="panel quality-panel">


      <div className="panel-header">

        <div>

          <h2>
            Data Quality
          </h2>

          <p>
            Latest validation results.
          </p>

        </div>

      </div>


      {/* OVERALL QUALITY */}

      <div className="overall-quality">

        <div className="quality-score-header">

          <span>
            Overall Quality
          </span>

          <strong>

            {overall.quality_score}%

          </strong>

        </div>


        <div className="progress-track overall-track">

          <div
            className="progress-fill"
            style={{
              width:
                `${overall.quality_score}%`
            }}
          />

        </div>

      </div>


      {/* DATASET QUALITY */}

      <div className="dataset-quality-list">

        {summary.datasets.map(
          (dataset) => (

            <div
              className="dataset-quality"
              key={dataset.dataset}
            >

              <div className="dataset-quality-header">

                <span className="quality-dataset-name">

                  {dataset.dataset}

                </span>


                <span className="dataset-score">

                  {dataset.quality_score}%

                </span>

              </div>


              <div className="progress-track">

                <div
                  className="progress-fill"
                  style={{
                    width:
                      `${dataset.quality_score}%`
                  }}
                />

              </div>

            </div>

          )
        )}

      </div>


      <div className="quality-footer">

        Quality scores reflect the latest
        validation pipeline results.

      </div>

    </section>

  );
}


export default Dashboard;