import { useEffect, useState } from "react";
import axios from "axios";
import Dashboard from "./components/Dashboard";
import "./App.css";

const API_URL = "http://localhost:8000";

const datasets = {
  equity: [
    { name: "trade_id", type: "integer" },
    { name: "instrument", type: "string" },
    { name: "quantity", type: "integer" },
    { name: "price", type: "float" },
    { name: "trade_date", type: "string" },
  ],

  bond: [
    { name: "trade_id", type: "integer" },
    { name: "bond_id", type: "string" },
    { name: "face_value", type: "float" },
    { name: "price", type: "float" },
  ],

  commodity: [
    { name: "trade_id", type: "integer" },
    { name: "commodity", type: "string" },
    { name: "quantity", type: "integer" },
    { name: "price", type: "float" },
  ],
};

function App() {
  const [dataset, setDataset] = useState("");
  const [columnName, setColumnName] = useState("");
  const [columnType, setColumnType] = useState("");
  const [operator, setOperator] = useState("");
  const [value, setValue] = useState("");

  const [rules, setRules] = useState([]);
  const [summary, setSummary] = useState(null);
  const [message, setMessage] = useState("");
  const [pipelineStatus, setPipelineStatus] = useState("ready");
  const [isRunning, setIsRunning] = useState(false);

  // ==========================================
  // Fetch Existing Rules
  // ==========================================

  const fetchRules = async () => {
    try {
      const response = await axios.get(
        `${API_URL}/rules`
      );

      setRules(response.data);
    } catch (error) {
      console.error(
        "Failed to load rules:",
        error
      );
    }
  };

  // ==========================================
  // Fetch Quality Summary
  // ==========================================

  const fetchQualitySummary = async () => {
    try {
      const response = await axios.get(
        `${API_URL}/quality-summary`
      );

      setSummary(response.data);
    } catch (error) {
      console.error(
        "Failed to load quality summary:",
        error
      );
    }
  };

  useEffect(() => {
    fetchRules();
    fetchQualitySummary();
  }, []);

  // ==========================================
  // Dataset Change
  // ==========================================

  const handleDatasetChange = (event) => {
    setDataset(event.target.value);
    setColumnName("");
    setColumnType("");
  };

  // ==========================================
  // Column Change
  // ==========================================

  const handleColumnChange = (event) => {
    const selectedColumn =
      event.target.value;

    setColumnName(selectedColumn);

    const column =
      datasets[dataset]?.find(
        (item) =>
          item.name === selectedColumn
      );

    if (column) {
      setColumnType(column.type);
    }
  };

  // ==========================================
  // Create Rule
  // ==========================================

  const handleSubmit = async (event) => {
    event.preventDefault();

    setMessage("");

    const rule = {
      dataset: dataset,
      column_name: columnName,
      column_type: columnType,
      operator: operator,
      value: value,
    };

    try {
      await axios.post(
        `${API_URL}/rules`,
        rule
      );

      setMessage(
        "Validation rule saved."
      );

      setOperator("");
      setValue("");

      await fetchRules();

      setTimeout(() => {
        setMessage("");
      }, 2500);

    } catch (error) {
      console.error(error);

      setMessage(
        "Failed to save rule."
      );
    }
  };

  // ==========================================
  // Delete Rule
  // ==========================================

  const deleteRule = async (ruleId) => {
    try {
      await axios.delete(
        `${API_URL}/rules/${ruleId}`
      );

      setMessage(
        "Validation rule deleted."
      );

      await fetchRules();

      setTimeout(() => {
        setMessage("");
      }, 2500);

    } catch (error) {
      console.error(error);

      setMessage(
        "Failed to delete rule."
      );
    }
  };

  const runPipeline = async () => {
  try {
    setIsRunning(true);
    setPipelineStatus("running");

    // Trigger Airflow pipeline
    const response = await axios.post(
      `${API_URL}/run-pipeline`
    );

    const dagRunId =
      response.data.dag_run_id;

    // Check pipeline status every 3 seconds
    const interval = setInterval(
      async () => {
        try {
          const statusResponse =
            await axios.get(
              `${API_URL}/pipeline-status/${encodeURIComponent(
                dagRunId
              )}`
            );

          const state =
            statusResponse.data.state;

          setPipelineStatus(state);

          // Pipeline completed successfully
          if (state === "success") {
            clearInterval(interval);

            setIsRunning(false);
            setPipelineStatus("success");

            // Get latest quality scores
            await fetchQualitySummary();
          }

          // Pipeline failed
          if (state === "failed") {
            clearInterval(interval);

            setIsRunning(false);
            setPipelineStatus("failed");
          }
        } catch (error) {
          console.error(
            "Failed to check pipeline status:",
            error
          );
        }
      },
      3000
    );

  } catch (error) {
    console.error(
      "Failed to start pipeline:",
      error
    );

    setPipelineStatus("failed");
    setIsRunning(false);
  }
};

  return (
    <div className="app-shell">

      <div className="app-container">

        {/* HEADER */}

        <header className="app-header">

          <div>

            <h1>
              Data Quality Pipeline
            </h1>

            <p>
              Configure validation rules and monitor
              data quality across your datasets.
            </p>

          </div>

          <div className="header-actions">

  <div
    className={`system-status status-${pipelineStatus}`}
  >
    <span className="status-dot"></span>

    {pipelineStatus === "ready" && "Ready"}

    {pipelineStatus === "queued" && "Queued"}

    {pipelineStatus === "running" && "Running"}

    {pipelineStatus === "success" && "Completed"}

    {pipelineStatus === "failed" && "Failed"}
  </div>


  <button
    className="run-pipeline-button"
    onClick={runPipeline}
    disabled={isRunning}
  >
    {isRunning
      ? "Running Pipeline..."
      : "Run Pipeline"}
  </button>

</div>

        </header>


        {/* CREATE RULE */}

        <section className="create-rule-section">

          <div className="section-heading">

            <div>

              <h2>
                Create Validation Rule
              </h2>

              <p>
                Define the conditions your data
                must satisfy.
              </p>

            </div>

          </div>


          <form
            className="rule-form"
            onSubmit={handleSubmit}
          >

            <div className="field">

              <label>
                Dataset
              </label>

              <select
                value={dataset}
                onChange={
                  handleDatasetChange
                }
                required
              >

                <option value="">
                  Select dataset
                </option>

                <option value="equity">
                  Equity
                </option>

                <option value="bond">
                  Bond
                </option>

                <option value="commodity">
                  Commodity
                </option>

              </select>

            </div>


            <div className="field">

              <label>
                Column
              </label>

              <select
                value={columnName}
                onChange={
                  handleColumnChange
                }
                disabled={!dataset}
                required
              >

                <option value="">
                  Select column
                </option>

                {dataset &&
                  datasets[dataset].map(
                    (column) => (

                      <option
                        key={column.name}
                        value={column.name}
                      >
                        {column.name}
                      </option>

                    )
                  )}

              </select>

            </div>


            <div className="field">

              <label>
                Type
              </label>

              <input
                type="text"
                value={columnType}
                placeholder="Auto"
                readOnly
              />

            </div>


            <div className="field">

              <label>
                Operator
              </label>

              <select
                value={operator}
                onChange={(event) =>
                  setOperator(
                    event.target.value
                  )
                }
                required
              >

                <option value="">
                  Select
                </option>

                <option value=">">
                  Greater than (&gt;)
                </option>

                <option value="<">
                  Less than (&lt;)
                </option>

                <option value=">=">
                  Greater or equal (&gt;=)
                </option>

                <option value="<=">
                  Less or equal (&lt;=)
                </option>

                <option value="==">
                  Equal (==)
                </option>

                <option value="!=">
                  Not equal (!=)
                </option>

              </select>

            </div>


            <div className="field">

              <label>
                Value
              </label>

              <input
                type="text"
                value={value}
                onChange={(event) =>
                  setValue(
                    event.target.value
                  )
                }
                placeholder="Enter value"
                required
              />

            </div>


            <div className="button-field">

              <button
                type="submit"
                className="add-rule-button"
              >

                + Add Rule

              </button>

            </div>

          </form>


          {message && (

            <div className="form-message">

              {message}

            </div>

          )}

        </section>


        {/* BOTTOM CONTENT */}

        <main className="main-content">


          {/* VALIDATION RULES */}

          <section className="panel rules-panel">

            <div className="panel-header">

              <div>

                <h2>
                  Validation Rules
                </h2>

                <p>
                  Rules currently applied
                  to your datasets.
                </p>

              </div>


              <span className="rule-count">

                {rules.length}
                {" "}
                {rules.length === 1
                  ? "rule"
                  : "rules"}

              </span>

            </div>


            <div className="rules-table-header">

              <span>
                Dataset
              </span>

              <span>
                Column
              </span>

              <span>
                Type
              </span>

              <span>
                Rule
              </span>

              <span>
                Action
              </span>

            </div>


            <div className="rules-list">

              {rules.length === 0 ? (

                <div className="empty-state">

                  No validation rules created.

                </div>

              ) : (

                rules.map((rule) => (

                  <div
                    className="rule-row"
                    key={rule.id}
                  >

                    <span className="dataset-name">

                      {rule.dataset}

                    </span>


                    <span>

                      {rule.column_name}

                    </span>


                    <span className="type-badge">

                      {rule.column_type}

                    </span>


                    <span className="condition">

                      {rule.operator}
                      {" "}
                      {rule.value}

                    </span>


                    <button
                      className="delete-button"
                      onClick={() =>
                        deleteRule(
                          rule.id
                        )
                      }
                    >

                      Delete

                    </button>

                  </div>

                ))

              )}

            </div>

          </section>


          {/* DATA QUALITY */}

          <Dashboard
            summary={summary}
          />

        </main>

      </div>

    </div>
  );
}

export default App;