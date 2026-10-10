// EDIT THESE to activate your contact buttons. Leave empty to hide a button.
const profile = {
  email: 'ferreira.dylan98@gmail.com',
  github: 'https://github.com/Similur',
  linkedin: 'https://www.linkedin.com/in/dylanferreirais/'
};

const projectDetails = {
  canvas: {
    kicker: 'PROJECT 01 · DATA INTEGRATION',
    title: 'Canvas Data 2 Integration',
    intro: 'I took over a learning platform data integration and put its recurring syncs into a consistent workflow.',
    challenge: 'Moving to Canvas Data 2 meant changing how we extracted and synced data. Large entities, especially submissions, could take a long time to sync and sometimes needed troubleshooting.',
    contribution: 'I took over the integration and wrote daily sync scripts in Microsoft Fabric using Instructure’s Python library. I maintained the workflows, checked the data, and investigated slow or failed syncs, working with vendor support when needed.',
    outcome: 'Reporting teams had a daily integration they could use for learning platform data, along with repeatable ways to check the data and troubleshoot issues.',
    disclosure: 'This case study describes my professional work. It leaves out the original source code, institutional data, credentials, and internal architecture.',
    codeSnippet: `# Canvas Data 2 Fabric Ingestion Pattern
from canvas_data2 import CanvasDataClient
from pyspark.sql.functions import current_timestamp, lit

client = CanvasDataClient(api_key=API_KEY, api_secret=API_SECRET)
tables = ["submissions", "courses", "users"]

for table in tables:
    df_snapshot = client.fetch_table(table)
    df_bronze = (
        spark.createDataFrame(df_snapshot)
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit("Canvas_Data_2"))
    )
    df_bronze.write.format("delta").mode("append").saveAsTable(f"bronze_{table}")`
  },
  fabric: {
    kicker: 'PROJECT 02 · DATA AUTOMATION',
    title: 'Automated Fabric Lakehouse Pipeline',
    intro: 'I used Microsoft Fabric, PySpark, and Delta tables to automate recurring financial and operational imports.',
    challenge: 'Roughly 30 daily month-to-date and year-to-date Excel extracts contained overlapping records and shifting formats, requiring manual reconciliation to ensure continuity with legacy imports.',
    contribution: 'I built PySpark notebooks in Fabric to ingest and standardize disparate workbooks, applied custom MTD and YTD business logic, and landed cleaned data from Bronze to Silver Delta tables. I then validated record counts and totals in Power BI against legacy imports.',
    outcome: 'Replaced manual daily file consolidation with an automated, idempotent pipeline, saving team hours and eliminating reporting discrepancies.',
    disclosure: 'This project comes from my previous professional work. Any code I publish later will use recreated logic and synthetic data.',
    codeSnippet: `# Multi-Workbook Consolidation & MTD/YTD Logic
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window

# 1. Standardize 30+ disparate Excel workbooks
df_workbooks = spark.read.format("excel").load("Files/daily_extracts/*.xlsx")

# 2. Windowed deduplication on composite primary key
window_spec = Window.partitionBy("account_id", "period").orderBy(col("file_timestamp").desc())
df_deduped = df_workbooks.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

# 3. Calculate running MTD/YTD aggregations
df_silver = df_deduped.withColumn("ytd_revenue", sum("amount").over(Window.partitionBy("fiscal_year")))\
                      .withColumn("mtd_revenue", sum("amount").over(Window.partitionBy("fiscal_month")))

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_financial_summaries")`
  },
  ml: {
    kicker: 'PROJECT 03 · APPLIED MACHINE LEARNING',
    title: 'Predictive Risk & Student Success Models',
    intro: 'I worked on predictive models to help operational teams spot risks earlier.',
    challenge: 'Support teams needed earlier warning that students might struggle or leave. Finance teams also needed a clearer view of repayment risk.',
    contribution: 'I built the models in Pecan AI, tested which available features were useful, and refined the inputs. I focused on signals teams could use, including student outcomes early in a course and financial repayment risk using a 120-day bad-debt reference point.',
    outcome: 'The predictions were intended to help teams intervene earlier and make better-informed operational decisions. Specific performance metrics are not shared publicly.',
    disclosure: 'This case study explains the approach. It does not include student-level records, training data, proprietary model artifacts, or unverified performance claims.',
    codeSnippet: `# Feature Extraction & Predictive Signal Scoring
from pyspark.sql.functions import col, when, datediff, current_date

features_df = (
    spark.table("silver_student_activity")
    .groupBy("student_id")
    .agg(
        datediff(current_date(), max("last_login_date")).alias("inactivity_days"),
        avg("assignment_score").alias("avg_performance"),
        count(when(col("score") < 70, 1)).alias("failed_assignments")
    )
    .withColumn("repayment_risk_flag", when(col("inactivity_days") > 120, 1).otherwise(0))
)

# Exported to Pecan AI for gradient boosted tree training`
  },
  quality: {
    kicker: 'PROJECT 04 · DATA VALIDATION',
    title: 'Automated Data Quality & Reconciliation',
    intro: 'I designed automated validation patterns used across production data migrations and lakehouse pipelines.',
    challenge: 'A pipeline can run completely error-free yet still drop records, produce silent duplicates, or misalign financial totals downstream.',
    contribution: 'I built automated reconciliation suites using SQL and PySpark that cross-check source-to-target record counts, validate schemas, detect duplicate keys, and verify aggregated metrics prior to downstream consumption.',
    outcome: 'Gave analysts and stakeholders confidence in migration cutovers by catching data drift and edge-case drops before data reached production reports.',
    disclosure: 'The design reflects patterns from my production work and adheres to clean architecture principles without disclosing proprietary assets.',
    codeSnippet: `# Automated Reconciliation & Data Quality Suite
def reconcile_tables(source_df, target_table_name, pkey):
    target_df = spark.table(target_table_name)
    
    # 1. Count & Row Check
    src_cnt, tgt_cnt = source_df.count(), target_df.count()
    assert src_cnt == tgt_cnt, f"Row count mismatch! Source: {src_cnt}, Target: {tgt_cnt}"
    
    # 2. Duplicate Check
    dupes = target_df.groupBy(pkey).count().filter("count > 1").count()
    assert dupes == 0, f"Detected {dupes} duplicate keys in {target_table_name}!"
    
    print(f"✓ Table {target_table_name} passed all reconciliation checks.")`
  },
  crm: {
    kicker: 'PROJECT 05 · REST API & LAKEHOUSE',
    title: 'CRM-to-Silver Lakehouse Ingestion',
    intro: 'I built an automated PySpark pipeline in Microsoft Fabric to ingest CRM contacts directly into OneLake.',
    challenge: 'Upstream CRM APIs frequently evolve, and pulling raw contacts directly into operational models risks pipeline breakage from schema drift, duplicates, and unhandled pagination limits.',
    contribution: 'I wrote Fabric PySpark notebooks that stage raw JSON payloads in a Bronze Delta table with ingestion audit metadata. I then extracted, cleaned, and deduplicated records using windowing functions before performing idempotent merges into the Silver Delta layer.',
    outcome: 'Eliminated manual CRM data exports and provided analysts with clean, query-ready Delta tables optimized via Z-ordering for fast downstream Power BI reporting.',
    disclosure: 'This case study reflects architectural patterns from my professional work. All endpoints, credentials, and customer records have been omitted.',
    codeSnippet: `# Idempotent Delta MERGE Pattern in Fabric
from delta.tables import DeltaTable

silver_tbl = DeltaTable.forName(spark, "silver_crm_contacts")

(
    silver_tbl.alias("target")
    .merge(
        source=df_clean_updates.alias("source"),
        condition="target.contact_id = source.contact_id"
    )
    .whenMatchedUpdate(
        condition="source.updated_at >= target.updated_at",
        set={
            "email": "source.email",
            "first_name": "source.first_name",
            "last_name": "source.last_name",
            "updated_at": "source.updated_at"
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)
# Lakehouse layout optimization
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
    codeSnippet: `# Azure Document Intelligence & NLP Ingestion
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential

client = DocumentAnalysisClient(endpoint=AZURE_ENDPOINT, credential=AzureKeyCredential(AZURE_KEY))

with open("survey_feedback_batch.pdf", "rb") as f:
    poller = client.begin_analyze_document("prebuilt-layout", f)
    result = poller.result()

feedback_entries = []
for page in result.pages:
    for line in page.lines:
        sentiment_score = analyze_nlp_sentiment(line.content)
        feedback_entries.append({"text": line.content, "sentiment": sentiment_score})

spark.createDataFrame(feedback_entries).write.format("delta").saveAsTable("silver_survey_intelligence")`
  }
};

const $ = id => document.getElementById(id);

function initPortfolio() {
  // 1. Footer Year
  const yearEl = $('year');
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  // 2. Project Category Filter Logic
  const filterBtns = document.querySelectorAll('.filter-btn');
  const projectCards = document.querySelectorAll('.project-grid .project-card');

  filterBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      filterBtns.forEach(b => b.classList.remove('active'));
      btn.classList.add('active');

      const filter = btn.dataset.filter;
      projectCards.forEach(card => {
        const cats = card.dataset.category || '';
        if (filter === 'all' || cats.includes(filter)) {
          card.classList.remove('is-hidden');
        } else {
          card.classList.add('is-hidden');
        }
      });
    });
  });

  // 3. Modal / Dialog Handler
  const dialog = $('project-dialog');
  const codeEl = $('dialog-code');
  const paneOverview = $('pane-overview');
  const paneCode = $('pane-code');
  const tabBtns = document.querySelectorAll('.dialog-tabs .tab-btn');

  // Modal Tab Switching
  tabBtns.forEach(tab => {
    tab.addEventListener('click', () => {
      tabBtns.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      const target = tab.dataset.tab;

      if (target === 'code') {
        paneOverview.classList.add('is-hidden');
        paneCode.classList.remove('is-hidden');
      } else {
        paneCode.classList.add('is-hidden');
        paneOverview.classList.remove('is-hidden');
      }
    });
  });

  document.addEventListener('click', (e) => {
    const trigger = e.target.closest('[data-project]');
    if (trigger) {
      e.preventDefault();
      const key = trigger.getAttribute('data-project');
      const item = projectDetails[key];
      if (!item) return;

      // Populate narrative text
      ['kicker', 'title', 'intro', 'challenge', 'contribution', 'outcome', 'disclosure'].forEach(field => {
        const el = $(`dialog-${field}`);
        if (el) el.textContent = item[field] || '';
      });

      // Populate code snippet
      if (codeEl) codeEl.textContent = item.codeSnippet || '# Code snippet pattern omitted.';

      // Reset to overview tab on open
      tabBtns.forEach(t => t.classList.toggle('active', t.dataset.tab === 'overview'));
      paneOverview.classList.remove('is-hidden');
      paneCode.classList.add('is-hidden');

      if (dialog) {
        typeof dialog.showModal === 'function' ? dialog.showModal() : dialog.setAttribute('open', '');
      }
      return;
    }

    if (e.target.closest('#dialog-close') && dialog) {
      dialog.close ? dialog.close() : dialog.removeAttribute('open');
      return;
    }

    if (e.target === dialog) {
      dialog.close ? dialog.close() : dialog.removeAttribute('open');
    }
  });

  // 4. Interactive Live Pipeline Simulator in Hero
  const simBtn = $('run-sim-btn');
  const simTerminal = $('sim-terminal');
  const simStatus = $('sim-status');
  const nodeIngest = $('sim-node-ingest');
  const nodeTransform = $('sim-node-transform');
  const nodeServe = $('sim-node-serve');

  if (simBtn && simTerminal) {
    let running = false;
    simBtn.addEventListener('click', () => {
      if (running) return;
      running = true;
      simBtn.disabled = true;
      simBtn.textContent = 'Running...';

      // Reset nodes
      [nodeIngest, nodeTransform, nodeServe].forEach(n => n.classList.remove('sim-active', 'active'));

      simTerminal.innerHTML = `<div><span class="code-muted"># initiating live pipeline run...</span></div>`;

      // Step 1: Ingest
      setTimeout(() => {
        nodeIngest.classList.add('sim-active');
        simTerminal.innerHTML += `<div><span class="code-blue">[0.12s] Ingesting:</span> Extracting 100 API contact records...</div>`;
      }, 400);

      // Step 2: Transform
      setTimeout(() => {
        nodeIngest.classList.remove('sim-active');
        nodeTransform.classList.add('sim-active');
        simTerminal.innerHTML += `<div><span class="code-blue">[0.38s] Transforming:</span> Window deduplication & schema validation...</div>`;
      }, 1200);

      // Step 3: Serve / Merge
      setTimeout(() => {
        nodeTransform.classList.remove('sim-active');
        nodeServe.classList.add('sim-active');
        simTerminal.innerHTML += `<div><span class="code-blue">[0.65s] Merging:</span> Executing idempotent Delta Lake MERGE...</div>`;
      }, 2000);

      // Step 4: Finished
      setTimeout(() => {
        nodeServe.classList.remove('sim-active');
        nodeTransform.classList.add('active'); // reset default active
        simTerminal.innerHTML += `<div class="code-success">✓ 100% reconciled. 0 duplicates. Delta tables published.</div>`;
        simBtn.disabled = false;
        simBtn.textContent = '↺ Re-run Pipeline';
        running = false;
      }, 2800);
    });
  }

  // 5. Mobile Nav Toggle
  const menu = document.querySelector('.menu-toggle');
  const nav = $('site-nav');
  if (menu && nav) {
    menu.addEventListener('click', () => {
      const open = nav.classList.toggle('open');
      menu.setAttribute('aria-expanded', String(open));
      menu.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');
    });

    nav.querySelectorAll('a').forEach(a => a.addEventListener('click', () => {
      nav.classList.remove('open');
      menu.setAttribute('aria-expanded', 'false');
      menu.setAttribute('aria-label', 'Open menu');
    }));
  }
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initPortfolio);
} else {
  initPortfolio();
}
