// EDIT THESE to activate your contact buttons. Leave empty to hide a button.
const profile = {
  email: 'ferreira.dylan98@gmail.com',[cite: 1]
  github: 'https://github.com/Similur',[cite: 1]
  linkedin: 'https://www.linkedin.com/in/dylanferreirais/'[cite: 1]
};

const projectDetails = {
  canvas: {
    kicker: 'PROJECT 01 · DATA INTEGRATION',[cite: 1]
    title: 'Canvas Data 2 Integration',[cite: 1]
    intro: 'I took over a learning platform data integration and put its recurring syncs into a consistent workflow.',[cite: 1]
    challenge: 'Moving to Canvas Data 2 meant changing how we extracted and synced data. Large entities, especially submissions, could take a long time to sync and sometimes needed troubleshooting.',[cite: 1]
    contribution: 'I took over the integration and wrote daily sync scripts in Microsoft Fabric using Instructure’s Python library. I maintained the workflows, checked the data, and investigated slow or failed syncs, working with vendor support when needed.',[cite: 1]
    outcome: 'Reporting teams had a daily integration they could use for learning platform data, along with repeatable ways to check the data and troubleshoot issues.',[cite: 1]
    disclosure: 'This case study describes my professional work. It leaves out the original source code, institutional data, credentials, and internal architecture.',[cite: 1]
    codeSnippet: `# Canvas Data 2 Daily Extraction Pattern
from canvas_data2 import CanvasDataClient
from pyspark.sql.functions import current_timestamp, lit

client = CanvasDataClient(api_key=API_KEY, api_secret=API_SECRET)
tables = ["submissions", "courses", "users"]

for tbl in tables:
    raw_batch = client.fetch_table(tbl)
    df_bronze = (
        spark.createDataFrame(raw_batch)
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit("Canvas_Data_2"))
    )
    df_bronze.write.format("delta").mode("append").saveAsTable(f"bronze_{tbl}")`
  },
  fabric: {
    kicker: 'PROJECT 02 · DATA AUTOMATION',
    title: 'Automated Fabric Lakehouse Pipeline',
    intro: 'I used Microsoft Fabric, PySpark, and Delta tables to automate recurring financial and operational imports.',
    challenge: 'Roughly 30 daily month-to-date and year-to-date Excel extracts contained overlapping records and shifting formats, requiring manual reconciliation to ensure continuity with legacy imports.',
    contribution: 'I built PySpark notebooks in Fabric to ingest and standardize disparate workbooks, applied custom MTD and YTD business logic, and landed cleaned data from Bronze to Silver Delta tables. I then validated record counts and totals in Power BI against legacy imports.',
    outcome: 'Replaced manual daily file consolidation with an automated, idempotent pipeline, saving team hours and eliminating reporting discrepancies.',
    disclosure: 'This project comes from my previous professional work. Any code I publish later will use recreated logic and synthetic data.',
    codeSnippet: `# Multi-Workbook Consolidation with Running Totals
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window

df_raw = spark.read.format("excel").load("Files/daily_extracts/*.xlsx")

# Windowed deduplication on composite key
dedup_window = Window.partitionBy("account_id", "period").orderBy(col("file_date").desc())
df_clean = df_raw.withColumn("rn", row_number().over(dedup_window)).filter("rn = 1").drop("rn")

# Calculate MTD and YTD metrics
df_silver = (
    df_clean
    .withColumn("ytd_total", sum("amount").over(Window.partitionBy("fiscal_year")))
    .withColumn("mtd_total", sum("amount").over(Window.partitionBy("fiscal_month")))
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_financial_summaries")`
  },
  ml: {
    kicker: 'PROJECT 03 · APPLIED MACHINE LEARNING',[cite: 1]
    title: 'Predictive Risk & Student Success Models',[cite: 1]
    intro: 'I worked on predictive models to help operational teams spot risks earlier.',[cite: 1]
    challenge: 'Support teams needed earlier warning that students might struggle or leave. Finance teams also needed a clearer view of repayment risk.',[cite: 1]
    contribution: 'I built the models in Pecan AI, tested which available features were useful, and refined the inputs. I focused on signals teams could use, including student outcomes early in a course and financial repayment risk using a 120-day bad-debt reference point.',[cite: 1]
    outcome: 'The predictions were intended to help teams intervene earlier and make better-informed operational decisions. Specific performance metrics are not shared publicly.',[cite: 1]
    disclosure: 'This case study explains the approach. It does not include student-level records, training data, proprietary model artifacts, or unverified performance claims.',[cite: 1]
    codeSnippet: `# Risk Signal & Feature Preparation
from pyspark.sql.functions import col, when, datediff, current_date, avg

df_features = (
    spark.table("silver_student_records")
    .groupBy("student_id")
    .agg(
        datediff(current_date(), max("last_interaction")).alias("days_inactive"),
        avg("quiz_score").alias("score_avg")
    )
    .withColumn("repayment_risk_flag", when(col("days_inactive") > 120, 1).otherwise(0))
)

# Features passed downstream to Pecan AI for training`
  },
  quality: {
    kicker: 'PROJECT 04 · DATA VALIDATION',
    title: 'Automated Data Quality & Reconciliation',
    intro: 'I designed automated validation patterns used across production data migrations and lakehouse pipelines.',
    challenge: 'A pipeline can run completely error-free yet still drop records, produce silent duplicates, or misalign financial totals downstream.',
    contribution: 'I built automated reconciliation suites using SQL and PySpark that cross-check source-to-target record counts, validate schemas, detect duplicate keys, and verify aggregated metrics prior to downstream consumption.',
    outcome: 'Gave analysts and stakeholders confidence in migration cutovers by catching data drift and edge-case drops before data reached production reports.',
    disclosure: 'The design reflects patterns from my production work and adheres to clean architecture principles without disclosing proprietary assets.',
    codeSnippet: `# Automated Reconciliation Suite
def run_reconciliation(source_df, target_table_name, key_col):
    target_df = spark.table(target_table_name)
    
    # 1. Row count match check
    assert source_df.count() == target_df.count(), "Row counts do not match!"
    
    # 2. Duplicate detection check
    dupe_count = target_df.groupBy(key_col).count().filter("count > 1").count()
    assert dupe_count == 0, f"Found {dupe_count} duplicate keys!"
    
    print(f"✓ All quality validations passed for {target_table_name}.")`
  },
  crm: {
    kicker: 'PROJECT 05 · REST API & LAKEHOUSE',
    title: 'CRM-to-Silver Lakehouse Ingestion',
    intro: 'I built an automated PySpark pipeline in Microsoft Fabric to ingest CRM contacts directly into OneLake.',
    challenge: 'Upstream CRM APIs frequently evolve, and pulling raw contacts directly into operational models risks pipeline breakage from schema drift, duplicates, and unhandled pagination limits.',
    contribution: 'I wrote Fabric PySpark notebooks that stage raw JSON payloads in a Bronze Delta table with ingestion audit metadata. I then extracted, cleaned, and deduplicated records using windowing functions before performing idempotent merges into the Silver Delta layer.',
    outcome: 'Eliminated manual CRM data exports and provided analysts with clean, query-ready Delta tables optimized via Z-ordering for fast downstream Power BI reporting.',
    disclosure: 'This case study reflects architectural patterns from my professional work. All endpoints, credentials, and customer records have been omitted.',
    codeSnippet: `# Idempotent Delta MERGE in Fabric
from delta.tables import DeltaTable

silver_tbl = DeltaTable.forName(spark, "silver_crm_contacts")

(
    silver_tbl.alias("target")
    .merge(
        source=df_clean.alias("source"),
        condition="target.contact_id = source.contact_id"
    )
    .whenMatchedUpdate(
        condition="source.updated_at >= target.updated_at",
        set={
            "email": "source.email",
            "name": "source.name",
            "updated_at": "source.updated_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)
spark.sql("OPTIMIZE silver_crm_contacts ZORDER BY (contact_id)")`
  },
  docintel: {
    kicker: 'PROJECT 06 · AI & DOCUMENT INTELLIGENCE',
    title: 'Survey Sentiment & Executive Intelligence',
    intro: 'I built an automated processing pipeline using Azure Document Intelligence to surface actionable survey insights for leadership.',
    challenge: 'Valuable feedback arrived trapped in unstructured documents and survey forms, making it time-consuming for leadership to identify trends or take prompt action.',
    contribution: 'I built Python extraction workflows integrated with Azure Document Intelligence to parse unstructured text, run NLP sentiment scoring, and structure key themes into standardized reporting datasets.',
    outcome: 'Turned qualitative survey responses into quantifiable trends and executive-ready summaries, giving decision-makers immediate visibility into operational sentiment.',
    disclosure: 'This project describes my technical approach. Proprietary survey instruments, individual responses, and internal models are excluded.',
    codeSnippet: `# Document Extraction & NLP Scoring Pipeline
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential

client = DocumentAnalysisClient(AZURE_ENDPOINT, AzureKeyCredential(AZURE_KEY))

with open("batch_feedback.pdf", "rb") as doc:
    poller = client.begin_analyze_document("prebuilt-layout", doc)
    result = poller.result()

parsed_items = []
for page in result.pages:
    for line in page.lines:
        sentiment = score_text_sentiment(line.content)
        parsed_items.append({"feedback": line.content, "score": sentiment})

spark.createDataFrame(parsed_items).write.format("delta").saveAsTable("silver_survey_intelligence")`
  }
};

const $ = id => document.getElementById(id);[cite: 1]

// Safe Global Event Delegation
document.addEventListener('click', (e) => {
  // 1. FILTER PILLS
  const filterBtn = e.target.closest('.filter-btn');
  if (filterBtn) {
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    filterBtn.classList.add('active');

    const filter = filterBtn.dataset.filter;
    document.querySelectorAll('.project-grid .project-card').forEach(card => {
      const cats = card.dataset.category || '';
      if (filter === 'all' || cats.includes(filter)) {
        card.classList.remove('is-hidden');
      } else {
        card.classList.add('is-hidden');
      }
    });
    return;
  }

  // 2. LIVE SIMULATOR BUTTON
  const simBtn = e.target.closest('#run-sim-btn');
  if (simBtn) {
    const simTerminal = $('sim-terminal');
    const nodeIngest = $('sim-node-ingest');
    const nodeTransform = $('sim-node-transform');
    const nodeServe = $('sim-node-serve');

    if (!simTerminal) return;

    simBtn.disabled = true;
    simBtn.textContent = 'Running...';

    // Clear state
    [nodeIngest, nodeTransform, nodeServe].forEach(n => {
      if (n) n.classList.remove('sim-active', 'active');
    });

    simTerminal.innerHTML = `<div><span class="code-muted"># initiating live pipeline run...</span></div>`;

    setTimeout(() => {
      if (nodeIngest) nodeIngest.classList.add('sim-active');
      simTerminal.innerHTML += `<div><span class="code-blue">[0.12s] Ingesting:</span> Fetching CRM API payload (100 records)...</div>`;
    }, 400);

    setTimeout(() => {
      if (nodeIngest) nodeIngest.classList.remove('sim-active');
      if (nodeTransform) nodeTransform.classList.add('sim-active');
      simTerminal.innerHTML += `<div><span class="code-blue">[0.38s] Transforming:</span> Window deduplication & schema validation...</div>`;
    }, 1100);

    setTimeout(() => {
      if (nodeTransform) nodeTransform.classList.remove('sim-active');
      if (nodeServe) nodeServe.classList.add('sim-active');
      simTerminal.innerHTML += `<div><span class="code-blue">[0.65s] Merging:</span> Executing idempotent Delta Lake MERGE...</div>`;
    }, 1800);

    setTimeout(() => {
      if (nodeServe) nodeServe.classList.remove('sim-active');
      if (nodeTransform) nodeTransform.classList.add('active');
      simTerminal.innerHTML += `<div class="code-success">✓ 100% reconciled. 0 duplicates. Delta tables published.</div>`;
      simBtn.disabled = false;
      simBtn.textContent = '↺ Re-run Pipeline';
    }, 2600);
    return;
  }

  // 3. MODAL OPEN (Case Studies)
  const caseStudyTrigger = e.target.closest('[data-project]');
  if (caseStudyTrigger) {
    e.preventDefault();
    const projectKey = caseStudyTrigger.getAttribute('data-project');
    const item = projectDetails[projectKey];
    const dialog = $('project-dialog');
    if (!item || !dialog) return;

    ['kicker', 'title', 'intro', 'challenge', 'contribution', 'outcome', 'disclosure'].forEach(key => {
      const el = $(`dialog-${key}`);
      if (el) el.textContent = item[key] || '';
    });

    const codeEl = $('dialog-code');
    if (codeEl) codeEl.textContent = item.codeSnippet || '# Code snippet pattern omitted.';

    // Reset tab to overview
    document.querySelectorAll('.dialog-tabs .tab-btn').forEach(t => {
      t.classList.toggle('active', t.dataset.tab === 'overview');
    });
    const paneOverview = $('pane-overview');
    const paneCode = $('pane-code');
    if (paneOverview) paneOverview.classList.remove('is-hidden');
    if (paneCode) paneCode.classList.add('is-hidden');

    typeof dialog.showModal === 'function' ? dialog.showModal() : dialog.setAttribute('open', '');
    return;
  }

  // 4. MODAL TABS (Overview vs Code)
  const tabBtn = e.target.closest('.dialog-tabs .tab-btn');
  if (tabBtn) {
    document.querySelectorAll('.dialog-tabs .tab-btn').forEach(t => t.classList.remove('active'));
    tabBtn.classList.add('active');

    const isCode = tabBtn.dataset.tab === 'code';
    const paneOverview = $('pane-overview');
    const paneCode = $('pane-code');

    if (paneOverview && paneCode) {
      paneOverview.classList.toggle('is-hidden', isCode);
      paneCode.classList.toggle('is-hidden', !isCode);
    }
    return;
  }

  // 5. MODAL CLOSE
  const dialog = $('project-dialog');
  if (dialog) {
    if (e.target.closest('#dialog-close') || e.target === dialog) {
      dialog.close ? dialog.close() : dialog.removeAttribute('open');
    }
  }
});

// Setup static year and contact buttons immediately
document.addEventListener('DOMContentLoaded', () => {
  const yearEl = $('year');
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  // Mobile menu toggle
  const menu = document.querySelector('.menu-toggle');
  const nav = $('site-nav');
  if (menu && nav) {
    menu.addEventListener('click', () => {
      const open = nav.classList.toggle('open');
      menu.setAttribute('aria-expanded', String(open));
      menu.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');
    });
    nav.querySelectorAll('a').forEach(a => {
      a.addEventListener('click', () => {
        nav.classList.remove('open');
        menu.setAttribute('aria-expanded', 'false');
        menu.setAttribute('aria-label', 'Open menu');
      });
    });
  }
});
