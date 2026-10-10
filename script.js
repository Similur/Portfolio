// --- Profile Config ---
const profile = {
  email: 'ferreira.dylan98@gmail.com',
  github: 'https://github.com/Similur',
  linkedin: 'https://www.linkedin.com/in/dylanferreirais/'
};

// --- Project Data ---
const projectDetails = {
  canvas: {
    kicker: 'PROJECT 01 · DATA INTEGRATION',
    title: 'Canvas Data 2 Integration',
    intro: 'I took over a learning platform data integration and put its recurring syncs into a consistent workflow.',
    challenge: 'Moving to Canvas Data 2 meant changing how we extracted and synced data. Large entities, especially submissions, could take a long time to sync and sometimes needed troubleshooting.',
    contribution: 'I took over the integration and wrote daily sync scripts in Microsoft Fabric using Instructure’s Python library. I maintained the workflows, checked the data, and investigated slow or failed syncs, working with vendor support when needed.',
    outcome: 'Reporting teams had a daily integration they could use for learning platform data, along with repeatable ways to check the data and troubleshoot issues.',
    disclosure: 'This case study describes my professional work. It leaves out the original source code, institutional data, credentials, and internal architecture.',
    codeSnippet: `# Canvas Data 2 (DAP) Ingestion Workflow in Microsoft Fabric
# Pattern: Automated daily sync using the official DAP client into OneLake Delta

import os
from instructure_dap.client import DAPClient
from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

# Step 1: Initialize the DAP client using environment credentials
# (In production Fabric notebooks, pull these securely via Key Vault / mssparkutils)
dap_client = DAPClient(
    base_url="https://api-gateway.instructure.com",
    client_id=os.getenv("DAP_CLIENT_ID"),
    client_secret=os.getenv("DAP_CLIENT_SECRET")
)

# Step 2: Download the latest snapshot files for heavy LMS entities
target_table = "submissions"
output_directory = f"/lakehouse/default/Files/raw_dap/{target_table}"

# Stream snapshot partitions locally to Lakehouse storage to avoid driver memory pressure
dap_client.download_table(
    namespace="canvas",
    table_name=target_table,
    output_directory=output_directory,
    file_format="json"
)

# Step 3: Ingest into Spark and land raw records into Bronze Delta
df_raw = spark.read.json(f"Files/raw_dap/{target_table}/*.json")

df_bronze = (
    df_raw
    .withColumn("ingested_at", current_timestamp())
    .withColumn("source_system", lit("canvas_dap"))
)

df_bronze.write.format("delta").mode("append").saveAsTable(f"bronze_canvas_{target_table}")

# Step 4: Deduplicate across overlapping partitions to keep the latest state for Silver
window_spec = Window.partitionBy("id").orderBy(col("updated_at").desc())

df_silver = (
    df_bronze
    .withColumn("row_rank", row_number().over(window_spec))
    .filter(col("row_rank") == 1)
    .drop("row_rank")
)

# Stage clean, query-ready records for analyst reporting
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"silver_canvas_{target_table}")`
  },
  fabric: {
    kicker: 'PROJECT 02 · DATA AUTOMATION',
    title: 'Automated Fabric Lakehouse Pipeline',
    intro: 'I used Microsoft Fabric, PySpark, and Delta tables to automate recurring financial and operational imports.',
    challenge: 'Roughly 30 daily month-to-date and year-to-date Excel extracts contained overlapping records and shifting formats, requiring manual reconciliation to ensure continuity with legacy imports.',
    contribution: 'I built PySpark notebooks in Fabric to ingest and standardize disparate workbooks, applied custom MTD and YTD business logic, and landed cleaned data from Bronze to Silver Delta tables. I then validated record counts and totals in Power BI against legacy imports.',
    outcome: 'Replaced manual daily file consolidation with an automated, idempotent pipeline, saving team hours and eliminating reporting discrepancies.',
    disclosure: 'This project comes from my previous professional work. Any code I publish later will use recreated logic and synthetic data.',
    codeSnippet: `# Multi-Workbook Consolidation & MTD/YTD Aggregations\nfrom pyspark.sql.functions import col, sum, row_number\nfrom pyspark.sql.window import Window\n\ndf_raw = spark.read.format("excel").load("Files/daily_extracts/*.xlsx")\n\ndedup_window = Window.partitionBy("account_id", "period").orderBy(col("file_date").desc())\ndf_clean = df_raw.withColumn("rn", row_number().over(dedup_window)).filter("rn = 1").drop("rn")\n\ndf_silver = (\n    df_clean\n    .withColumn("ytd_total", sum("amount").over(Window.partitionBy("fiscal_year")))\n    .withColumn("mtd_total", sum("amount").over(Window.partitionBy("fiscal_month")))\n)\n\ndf_silver.write.format("delta").mode("overwrite").saveAsTable("silver_financial_summaries")`
  },
  ml: {
    kicker: 'PROJECT 03 · APPLIED MACHINE LEARNING',
    title: 'Predictive Risk & Student Success Models',
    intro: 'I worked on predictive models to help operational teams spot risks earlier.',
    challenge: 'Support teams needed earlier warning that students might struggle or leave. Finance teams also needed a clearer view of repayment risk.',
    contribution: 'I built the models in Pecan AI, tested which available features were useful, and refined the inputs. I focused on signals teams could use, including student outcomes early in a course and financial repayment risk using a 120-day bad-debt reference point.',
    outcome: 'The predictions were intended to help teams intervene earlier and make better-informed operational decisions. Specific performance metrics are not shared publicly.',
    disclosure: 'This case study explains the approach. It does not include student-level records, training data, proprietary model artifacts, or unverified performance claims.',
    codeSnippet: `# Risk Signal Preparation\nfrom pyspark.sql.functions import col, when, datediff, current_date, avg\n\ndf_features = (\n    spark.table("silver_student_records")\n    .groupBy("student_id")\n    .agg(\n        datediff(current_date(), max("last_interaction")).alias("days_inactive"),\n        avg("quiz_score").alias("score_avg")\n    )\n    .withColumn("repayment_risk_flag", when(col("days_inactive") > 120, 1).otherwise(0))\n)`
  },
  quality: {
    kicker: 'PROJECT 04 · DATA VALIDATION',
    title: 'Automated Data Quality & Reconciliation',
    intro: 'I designed automated validation patterns used across production data migrations and lakehouse pipelines.',
    challenge: 'A pipeline can run completely error-free yet still drop records, produce silent duplicates, or misalign financial totals downstream.',
    contribution: 'I built automated reconciliation suites using SQL and PySpark that cross-check source-to-target record counts, validate schemas, detect duplicate keys, and verify aggregated metrics prior to downstream consumption.',
    outcome: 'Gave analysts and stakeholders confidence in migration cutovers by catching data drift and edge-case drops before data reached production reports.',
    disclosure: 'The design reflects patterns from my production work and adheres to clean architecture principles without disclosing proprietary assets.',
    codeSnippet: `# Automated Reconciliation Suite\ndef run_reconciliation(source_df, target_table_name, key_col):\n    target_df = spark.table(target_table_name)\n    assert source_df.count() == target_df.count(), "Row counts do not match!"\n    dupe_count = target_df.groupBy(key_col).count().filter("count > 1").count()\n    assert dupe_count == 0, f"Found {dupe_count} duplicate keys!"\n    print(f"✓ Validations passed for {target_table_name}")`
  },
  crm: {
    kicker: 'PROJECT 05 · REST API & LAKEHOUSE',
    title: 'CRM-to-Silver Lakehouse Ingestion',
    intro: 'I built an automated PySpark pipeline in Microsoft Fabric to ingest CRM contacts directly into OneLake.',
    challenge: 'Upstream CRM APIs frequently evolve, and pulling raw contacts directly into operational models risks pipeline breakage from schema drift, duplicates, and unhandled pagination limits.',
    contribution: 'I wrote Fabric PySpark notebooks that stage raw JSON payloads in a Bronze Delta table with ingestion audit metadata. I then extracted, cleaned, and deduplicated records using windowing functions before performing idempotent merges into the Silver Delta layer.',
    outcome: 'Eliminated manual CRM data exports and provided analysts with clean, query-ready Delta tables optimized via Z-ordering for fast downstream Power BI reporting.',
    disclosure: 'This case study reflects architectural patterns from my professional work. All endpoints, credentials, and customer records have been omitted.',
    codeSnippet: `# Idempotent Delta MERGE in Microsoft Fabric\nfrom delta.tables import DeltaTable\n\nsilver_tbl = DeltaTable.forName(spark, "silver_crm_contacts")\n(\n    silver_tbl.alias("target")\n    .merge(\n        source=df_clean.alias("source"),\n        condition="target.contact_id = source.contact_id"\n    )\n    .whenMatchedUpdate(\n        condition="source.updated_at >= target.updated_at",\n        set={"email": "source.email", "name": "source.name", "updated_at": "source.updated_at"}\n    )\n    .whenNotMatchedInsertAll()\n    .execute()\n)\nspark.sql("OPTIMIZE silver_crm_contacts ZORDER BY (contact_id)")`
  },
  docintel: {
    kicker: 'PROJECT 06 · AI & DOCUMENT INTELLIGENCE',
    title: 'Survey Sentiment & Executive Intelligence',
    intro: 'I built an automated processing pipeline using Azure Document Intelligence to surface actionable survey insights for leadership.',
    challenge: 'Valuable feedback arrived trapped in unstructured documents and survey forms, making it time-consuming for leadership to identify trends or take prompt action.',
    contribution: 'I built Python extraction workflows integrated with Azure Document Intelligence to parse unstructured text, run NLP sentiment scoring, and structure key themes into standardized reporting datasets.',
    outcome: 'Turned qualitative survey responses into quantifiable trends and executive-ready summaries, giving decision-makers immediate visibility into operational sentiment.',
    disclosure: 'This project describes my technical approach. Proprietary survey instruments, individual responses, and internal models are excluded.',
    codeSnippet: `# Document Extraction & NLP Scoring Pipeline\nfrom azure.ai.formrecognizer import DocumentAnalysisClient\nfrom azure.core.credentials import AzureKeyCredential\n\nclient = DocumentAnalysisClient(AZURE_ENDPOINT, AzureKeyCredential(AZURE_KEY))\nwith open("batch_feedback.pdf", "rb") as doc:\n    poller = client.begin_analyze_document("prebuilt-layout", doc)\n    result = poller.result()\n\nparsed_items = []\nfor page in result.pages:\n    for line in page.lines:\n        sentiment = score_text_sentiment(line.content)\n        parsed_items.append({"feedback": line.content, "score": sentiment})\n\nspark.createDataFrame(parsed_items).write.format("delta").saveAsTable("silver_survey_intelligence")`
  }
};

// Global Event Handler - Never fails silently
document.addEventListener('click', function(event) {
  // 1. PROJECT FILTER PILLS
  const filterBtn = event.target.closest('.filter-btn');
  if (filterBtn) {
    event.preventDefault();
    try {
      document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
      filterBtn.classList.add('active');

      const filterVal = filterBtn.getAttribute('data-filter') || 'all';
      const cards = document.querySelectorAll('.project-grid .project-card');

      cards.forEach(card => {
        const categories = card.getAttribute('data-category') || '';
        if (filterVal === 'all' || categories.includes(filterVal)) {
          card.classList.remove('is-hidden');
          card.style.display = '';
        } else {
          card.classList.add('is-hidden');
          card.style.display = 'none';
        }
      });
      console.log('Filter applied:', filterVal);
    } catch (err) {
      console.error('Filter error:', err);
    }
    return;
  }

  // 2. LIVE SIMULATOR BUTTON
  const simBtn = event.target.closest('#run-sim-btn');
  if (simBtn) {
    event.preventDefault();
    try {
      const terminal = document.getElementById('sim-terminal');
      const nodeIngest = document.getElementById('sim-node-ingest');
      const nodeTransform = document.getElementById('sim-node-transform');
      const nodeServe = document.getElementById('sim-node-serve');

      if (!terminal) return;

      simBtn.disabled = true;
      simBtn.textContent = 'Running...';

      [nodeIngest, nodeTransform, nodeServe].forEach(n => {
        if (n) {
          n.classList.remove('sim-active', 'active');
        }
      });

      terminal.innerHTML = '<div><span class="code-muted"># initiating live pipeline run...</span></div>';

      setTimeout(() => {
        if (nodeIngest) nodeIngest.classList.add('sim-active');
        terminal.innerHTML += '<div><span class="code-blue">[0.12s] Ingesting:</span> Fetching CRM API payload (100 records)...</div>';
      }, 350);

      setTimeout(() => {
        if (nodeIngest) nodeIngest.classList.remove('sim-active');
        if (nodeTransform) nodeTransform.classList.add('sim-active');
        terminal.innerHTML += '<div><span class="code-blue">[0.38s] Transforming:</span> Window deduplication & schema validation...</div>';
      }, 1050);

      setTimeout(() => {
        if (nodeTransform) nodeTransform.classList.remove('sim-active');
        if (nodeServe) nodeServe.classList.add('sim-active');
        terminal.innerHTML += '<div><span class="code-blue">[0.65s] Merging:</span> Executing idempotent Delta Lake MERGE...</div>';
      }, 1750);

      setTimeout(() => {
        if (nodeServe) nodeServe.classList.remove('sim-active');
        if (nodeTransform) nodeTransform.classList.add('active');
        terminal.innerHTML += '<div class="code-success">✓ 100% reconciled. 0 duplicates. Delta tables published.</div>';
        simBtn.disabled = false;
        simBtn.textContent = '↺ Re-run Pipeline';
      }, 2500);
      console.log('Simulator executed');
    } catch (err) {
      console.error('Simulator error:', err);
    }
    return;
  }

  // 3. CASE STUDY MODAL OPEN TRIGGER
  const caseTrigger = event.target.closest('[data-project]');
  if (caseTrigger) {
    event.preventDefault();
    try {
      const projectKey = caseTrigger.getAttribute('data-project');
      const item = projectDetails[projectKey];
      const dialog = document.getElementById('project-dialog');

      if (!item || !dialog) {
        console.warn('Item or dialog missing for:', projectKey);
        return;
      }

      ['kicker', 'title', 'intro', 'challenge', 'contribution', 'outcome', 'disclosure'].forEach(key => {
        const el = document.getElementById(`dialog-${key}`);
        if (el) el.textContent = item[key] || '';
      });

      const codeEl = document.getElementById('dialog-code');
      if (codeEl) codeEl.textContent = item.codeSnippet || '# Code snippet pattern omitted.';

      // Reset to overview tab
      document.querySelectorAll('.dialog-tabs .tab-btn').forEach(btn => {
        btn.classList.toggle('active', btn.getAttribute('data-tab') === 'overview');
      });
      const pOverview = document.getElementById('pane-overview');
      const pCode = document.getElementById('pane-code');
      if (pOverview) {
        pOverview.classList.remove('is-hidden');
        pOverview.style.display = 'block';
      }
      if (pCode) {
        pCode.classList.add('is-hidden');
        pCode.style.display = 'none';
      }

      if (typeof dialog.showModal === 'function') {
        dialog.showModal();
      } else {
        dialog.setAttribute('open', '');
      }
      console.log('Opened case study modal for:', projectKey);
    } catch (err) {
      console.error('Modal open error:', err);
    }
    return;
  }

  // 4. MODAL TAB TOGGLE (Overview vs Code)
  const tabBtn = event.target.closest('.dialog-tabs .tab-btn');
  if (tabBtn) {
    event.preventDefault();
    try {
      document.querySelectorAll('.dialog-tabs .tab-btn').forEach(b => b.classList.remove('active'));
      tabBtn.classList.add('active');

      const isCode = tabBtn.getAttribute('data-tab') === 'code';
      const pOverview = document.getElementById('pane-overview');
      const pCode = document.getElementById('pane-code');

      if (pOverview && pCode) {
        if (isCode) {
          pOverview.classList.add('is-hidden');
          pOverview.style.display = 'none';
          pCode.classList.remove('is-hidden');
          pCode.style.display = 'block';
        } else {
          pCode.classList.add('is-hidden');
          pCode.style.display = 'none';
          pOverview.classList.remove('is-hidden');
          pOverview.style.display = 'block';
        }
      }
    } catch (err) {
      console.error('Tab switch error:', err);
    }
    return;
  }

  // 5. MODAL CLOSE TRIGGERS
  const closeBtn = event.target.closest('#dialog-close');
  const dialogEl = document.getElementById('project-dialog');
  if (closeBtn || event.target === dialogEl) {
    if (dialogEl) {
      if (typeof dialogEl.close === 'function') {
        dialogEl.close();
      }
      dialogEl.removeAttribute('open');
    }
  }
});

// Setup static DOM items safely
function setupPage() {
  const yearEl = document.getElementById('year');
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  const menu = document.querySelector('.menu-toggle');
  const nav = document.getElementById('site-nav');
  if (menu && nav) {
    menu.onclick = function() {
      const open = nav.classList.toggle('open');
      menu.setAttribute('aria-expanded', String(open));
      menu.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');
    };
    nav.querySelectorAll('a').forEach(a => {
      a.onclick = function() {
        nav.classList.remove('open');
        menu.setAttribute('aria-expanded', 'false');
      };
    });
  }
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', setupPage);
} else {
  setupPage();
}
