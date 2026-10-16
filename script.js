/**
 * Dylan Ferreira - Data Engineering Portfolio Engine
 * GitHub Pages Deployment (Vanilla ES6)
 */

// --- Profile Configuration ---
const profile = {
  name: 'Dylan Ferreira',
  title: 'Data Engineer // Azure & Microsoft Fabric Specialist',
  email: 'ferreira.dylan98@gmail.com',
  phone: '559-205-4802',
  github: 'https://github.com/Similur',
  linkedin: 'https://www.linkedin.com/in/dylanferreirais/'
};

// --- Enterprise Project Data Repository ---
const projectDetails = {
  canvas: {
    kicker: 'PROJECT 01 · DATA INTEGRATION',
    title: 'Canvas Data 2 Integration',
    intro: 'Engineered daily automated Canvas LMS synchronization, extracting and harmonizing 49 relational tables into OneLake Delta tables.',
    challenge: 'Transitioning to Canvas Data 2 (DAP) introduced strict API pagination, heavy submission payloads, and intermittent network timeouts requiring automated recovery.',
    contribution: 'Wrote robust daily extraction scripts in Microsoft Fabric using Instructure DAP Client. Configured batch partitioning, exponential backoff retries, and row-level reconciliation against upstream API counts.',
    outcome: 'Eliminated manual data movement and guaranteed 100% data parity across 49 tables, providing campus analytics teams with timely reporting tables.',
    disclosure: 'This case study describes production architectural patterns. Proprietary student records, institution tokens, and internal network endpoints are excluded.',
    codeSnippet: `# Canvas Data 2 (DAP) Ingestion Workflow in Microsoft Fabric
# Pattern: Automated daily sync using official DAP client into OneLake Delta

import os
from instructure_dap.client import DAPClient
from pyspark.sql.functions import col, current_timestamp, lit, row_number
from pyspark.sql.window import Window

# Step 1: Initialize DAP client with Azure Key Vault secrets
dap_client = DAPClient(
    base_url="https://api-gateway.instructure.com",
    client_id=os.getenv("DAP_CLIENT_ID"),
    client_secret=os.getenv("DAP_CLIENT_SECRET")
)

# Step 2: Stream partition downloads to avoid driver memory saturation
target_table = "submissions"
output_dir = f"/lakehouse/default/Files/raw_dap/{target_table}"

dap_client.download_table(
    namespace="canvas",
    table_name=target_table,
    output_directory=output_dir,
    file_format="json"
)

# Step 3: Read raw JSON into Spark and append to Bronze stage
df_raw = spark.read.json(f"Files/raw_dap/{target_table}/*.json")
df_bronze = (
    df_raw
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_system", lit("canvas_dap"))
)
df_bronze.write.format("delta").mode("append").saveAsTable(f"bronze_canvas_{target_table}")

# Step 4: Windowed deduplication across overlapping snapshot partitions
window_spec = Window.partitionBy("id").orderBy(col("updated_at").desc())
df_silver = (
    df_bronze
    .withColumn("row_rank", row_number().over(window_spec))
    .filter(col("row_rank") == 1)
    .drop("row_rank")
)

# Stage query-ready dataset for downstream semantic models
df_silver.write.format("delta").mode("overwrite").saveAsTable(f"silver_canvas_{target_table}")`
  },

  fabric: {
    kicker: 'PROJECT 02 · DATA PIPELINE AUTOMATION',
    title: 'Automated Fabric Lakehouse Pipeline',
    intro: 'Automated recurring financial and operational file ingestion using Microsoft Fabric, PySpark, and Delta Lake.',
    challenge: 'Approximately 30 daily MTD and YTD Excel extracts contained overlapping records, shifting column names, and manual reconciliation overhead totaling 2+ hours daily.',
    contribution: 'Built vectorized PySpark notebooks in Fabric to ingest disparate workbooks from Azure file shares, applied automated MTD/YTD reconciliation, and decoupled workloads into Bronze and Silver Delta tables.',
    outcome: 'Reduced daily processing from roughly 2 hours to 5 minutes (95% speed improvement) and completely eliminated downstream reporting discrepancies.',
    disclosure: 'Reconstructed technical approach based on production workflows; proprietary business rules and actual financial figures have been sanitized.',
    codeSnippet: `# Multi-Workbook Consolidation & MTD/YTD Aggregation Routine
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql.window import Window

# 1. Ingest multi-file vendor drop
df_raw = spark.read.format("excel").load("Files/daily_extracts/*.xlsx")

# 2. Reconcile overlapping MTD and YTD file snapshots
dedup_window = Window.partitionBy("account_id", "period").orderBy(col("file_date").desc())
df_clean = df_raw.withColumn("rn", row_number().over(dedup_window)).filter("rn = 1").drop("rn")

# 3. Calculate running fiscal aggregates
df_silver = (
    df_clean
    .withColumn("ytd_total", sum("amount").over(Window.partitionBy("fiscal_year")))
    .withColumn("mtd_total", sum("amount").over(Window.partitionBy("fiscal_month")))
)

# 4. Overwrite Silver analytical entity idempotently
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_financial_summaries")`
  },

  crm: {
    kicker: 'PROJECT 03 · REST API & LAKEHOUSE',
    title: 'CRM-to-Silver Lakehouse Ingestion',
    intro: 'Engineered high-throughput PySpark pipelines in Microsoft Fabric to ingest and harmonize 30+ CRM vendor data feeds.',
    challenge: 'Frequent vendor schema drift and direct loads into transactional SQL Server caused lock escalations, blocking queries, and frequent sync failures.',
    contribution: 'Designed a Medallion ingestion layer where raw JSON lands in append-only Bronze Delta tables, followed by atomic Delta merges into Silver using deterministic row hashes.',
    outcome: 'Eliminated transactional deadlocks, insulated reporting models from API contract shifts, and achieved sub-second query response times on published tables.',
    disclosure: 'All vendor endpoints, client keys, and student records have been omitted.',
    codeSnippet: `# Idempotent Delta MERGE with Z-Order Optimization
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

# Optimize storage clustering for downstream Power BI Direct Lake
spark.sql("OPTIMIZE silver_crm_contacts ZORDER BY (contact_id)")`
  },

  quality: {
    kicker: 'PROJECT 04 · DATA VALIDATION',
    title: 'Automated Data Quality & Reconciliation',
    intro: 'Built end-to-end automated assertion and validation suites protecting production Lakehouse migrations.',
    challenge: 'Pipelines can execute with HTTP 200/Success codes while silently omitting records, drifting data types, or skewing financial totals.',
    contribution: 'Authored automated Python and SQL assertion workflows comparing source flat files to Delta Lake outputs, asserting row counts, primary key uniqueness, and checksum parity.',
    outcome: 'Established complete stakeholder confidence by proving zero variance across modern Fabric outputs prior to legacy pipeline decommissioning.',
    disclosure: 'Sanitized assertion logic demonstrating core data quality and reconciliation principles.',
    codeSnippet: `# Automated Source-to-Target Data Quality Validator
def run_reconciliation(source_df, target_table_name, key_col):
    target_df = spark.table(target_table_name)
    
    # 1. Row count assertion
    assert source_df.count() == target_df.count(), "Row counts do not match!"
    
    # 2. Primary key uniqueness validation
    dupe_count = target_df.groupBy(key_col).count().filter("count > 1").count()
    assert dupe_count == 0, f"Integrity Failure: Found {dupe_count} duplicate keys!"
    
    print(f"✓ All pipeline assertions verified for {target_table_name}")`
  },

  ml: {
    kicker: 'PROJECT 05 · APPLIED MACHINE LEARNING',
    title: 'Predictive Risk & Retention Models',
    intro: 'Operationalized feature engineering and scoring pipelines for 4 predictive machine learning models in Pecan AI.',
    challenge: 'Campus advising teams lacked proactive early indicators to support students facing academic hurdles or repayment risk before critical milestones passed.',
    contribution: 'Aggregated historical student engagement, course interaction intervals, and demographic tables into feature stores, generating 120-day predictive risk flags.',
    outcome: 'Operationalized 4 predictive models into automated daily Power BI feeds, enabling academic staff to intervene with at-risk students proactively.',
    disclosure: 'Proprietary student identifiers, institutional training sets, and model weights have been omitted.',
    codeSnippet: `# Machine Learning Feature Store Preparation
from pyspark.sql.functions import col, when, datediff, current_date, avg, max

df_features = (
    spark.table("silver_student_records")
    .groupBy("student_id")
    .agg(
        datediff(current_date(), max("last_interaction")).alias("days_inactive"),
        avg("quiz_score").alias("score_avg")
    )
    .withColumn("repayment_risk_flag", when(col("days_inactive") > 120, 1).otherwise(0))
)

df_features.write.format("delta").mode("overwrite").saveAsTable("gold_student_risk_features")`
  },

  docintel: {
    kicker: 'PROJECT 06 · AI & DOCUMENT PROCESSING',
    title: 'Survey Sentiment & Document Intelligence',
    intro: 'Automated extraction and NLP sentiment workflows parsing thousands of unstructured campus documents.',
    challenge: 'Critical operational survey responses were trapped in unstructured PDFs and forms, requiring hours of manual data entry for quarterly reviews.',
    contribution: 'Engineered Python extraction pipelines using Azure Document Intelligence (Form Recognizer) to OCR unstructured text, apply sentiment scoring, and stage structured tables.',
    outcome: 'Transformed qualitative student surveys into quantifiable trend metrics, delivering immediate sentiment visibility to executive leadership.',
    disclosure: 'Sanitized pipeline logic. Proprietary survey instruments and personal responses are excluded.',
    codeSnippet: `# Azure Document Intelligence OCR & Sentiment Pipeline
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

// --- Single Global Event Delegation Listener ---
document.addEventListener('click', function(event) {

  // 1. FILTER PILLS
  const filterBtn = event.target.closest('.filter-btn');
  if (filterBtn) {
    event.preventDefault();
    document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
    filterBtn.classList.add('active');

    const filterVal = filterBtn.getAttribute('data-filter') || 'all';
    const cards = document.querySelectorAll('.project-grid .project-card');

    cards.forEach(card => {
      const categories = card.getAttribute('data-category') || '';
      if (filterVal === 'all' || categories.includes(filterVal)) {
        card.style.display = '';
      } else {
        card.style.display = 'none';
      }
    });
    return;
  }

  // 2. LIVE ARCHITECTURE SIMULATOR
  const simBtn = event.target.closest('#run-sim-btn');
  if (simBtn) {
    event.preventDefault();
    const terminal = document.getElementById('sim-terminal');
    const nodeIngest = document.getElementById('sim-node-ingest');
    const nodeBronze = document.getElementById('sim-node-bronze');
    const nodeTransform = document.getElementById('sim-node-transform');
    const nodeServe = document.getElementById('sim-node-serve');

    if (!terminal) return;

    simBtn.disabled = true;
    simBtn.innerHTML = '<span class="material-symbols-outlined icon-sm">hourglass_top</span> Executing...';

    [nodeIngest, nodeBronze, nodeTransform, nodeServe].forEach(n => {
      if (n) n.classList.remove('sim-active');
    });

    terminal.innerHTML = '<div class="code-font text-muted"># Initializing Medallion pipeline execution...</div>';

    setTimeout(() => {
      if (nodeIngest) nodeIngest.classList.add('sim-active');
      terminal.innerHTML += '<div class="code-font"><span class="text-accent-blue">[0.15s] Extract:</span> Polling REST API &amp; file drops (100k rows)...</div>';
    }, 350);

    setTimeout(() => {
      if (nodeIngest) nodeIngest.classList.remove('sim-active');
      if (nodeBronze) nodeBronze.classList.add('sim-active');
      terminal.innerHTML += '<div class="code-font"><span class="text-accent-teal">[0.42s] Bronze Stage:</span> Landing append-only Delta snapshots on ADLS Gen2...</div>';
    }, 1100);

    setTimeout(() => {
      if (nodeBronze) nodeBronze.classList.remove('sim-active');
      if (nodeTransform) nodeTransform.classList.add('sim-active');
      terminal.innerHTML += '<div class="code-font"><span class="text-accent-blue">[0.75s] Silver Conformed:</span> SHA-256 row-hash CDC &amp; idempotent PySpark merge...</div>';
    }, 1850);

    setTimeout(() => {
      if (nodeTransform) nodeTransform.classList.remove('sim-active');
      if (nodeServe) nodeServe.classList.add('sim-active');
      terminal.innerHTML += '<div class="code-font"><span class="text-accent-gold">[1.05s] Gold Analytics:</span> Z-Order OPTIMIZE &amp; Power BI Direct Lake streaming...</div>';
    }, 2600);

    setTimeout(() => {
      if (nodeServe) nodeServe.classList.remove('sim-active');
      terminal.innerHTML += '<div class="code-font text-accent-teal" style="font-weight:600; margin-top:0.35rem;">✓ Run Complete: 100% reconciled. 0 duplicates. Sub-second queries ready.</div>';
      simBtn.disabled = false;
      simBtn.innerHTML = '<span class="material-symbols-outlined icon-sm">refresh</span> <span>Re-run Pipeline</span>';
    }, 3400);
    return;
  }

  // 3. CASE STUDY MODAL OPEN TRIGGER
  const caseTrigger = event.target.closest('[data-project]');
  if (caseTrigger) {
    event.preventDefault();
    const projectKey = caseTrigger.getAttribute('data-project');
    const item = projectDetails[projectKey];
    const dialog = document.getElementById('project-dialog');

    if (!item || !dialog) return;

    ['kicker', 'title', 'intro', 'challenge', 'contribution', 'outcome', 'disclosure'].forEach(key => {
      const el = document.getElementById(`dialog-${key}`);
      if (el) el.textContent = item[key] || '';
    });

    const codeEl = document.getElementById('dialog-code');
    if (codeEl) codeEl.textContent = item.codeSnippet || '# Code snippet omitted.';

    // Default to 'overview' tab on open
    document.querySelectorAll('.dialog-tabs .tab-btn').forEach(btn => {
      btn.classList.toggle('active', btn.getAttribute('data-tab') === 'overview');
    });
    
    const pOverview = document.getElementById('pane-overview');
    const pCode = document.getElementById('pane-code');
    if (pOverview) pOverview.style.display = 'block';
    if (pCode) pCode.style.display = 'none';

    if (typeof dialog.showModal === 'function') {
      dialog.showModal();
    } else {
      dialog.setAttribute('open', '');
    }
    return;
  }

  // 4. MODAL TAB TOGGLE (Overview vs Code)
  const tabBtn = event.target.closest('.dialog-tabs .tab-btn');
  if (tabBtn) {
    event.preventDefault();
    document.querySelectorAll('.dialog-tabs .tab-btn').forEach(b => b.classList.remove('active'));
    tabBtn.classList.add('active');

    const isCode = tabBtn.getAttribute('data-tab') === 'code';
    const pOverview = document.getElementById('pane-overview');
    const pCode = document.getElementById('pane-code');

    if (pOverview && pCode) {
      pOverview.style.display = isCode ? 'none' : 'block';
      pCode.style.display = isCode ? 'block' : 'none';
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
    return;
  }

  // 6. COPY CODE FROM MODAL
  const copyModalBtn = event.target.closest('#dialog-copy-btn');
  if (copyModalBtn) {
    event.preventDefault();
    const codeContent = document.getElementById('dialog-code')?.textContent || '';
    navigator.clipboard.writeText(codeContent).then(() => {
      const originalText = copyModalBtn.innerHTML;
      copyModalBtn.innerHTML = '<span class="material-symbols-outlined icon-sm">check</span> Copied!';
      setTimeout(() => { copyModalBtn.innerHTML = originalText; }, 2500);
    });
    return;
  }

  // 7. COPY CODE FROM MAIN PYSPARK SECTION
  const copyMainBtn = event.target.closest('#copy-code-btn');
  if (copyMainBtn) {
    event.preventDefault();
    const mainCodeContent = document.getElementById('main-pyspark-code')?.textContent || '';
    navigator.clipboard.writeText(mainCodeContent).then(() => {
      const btnTextEl = document.getElementById('copy-btn-text');
      if (btnTextEl) {
        const prev = btnTextEl.textContent;
        btnTextEl.textContent = 'Copied to Clipboard!';
        setTimeout(() => { btnTextEl.textContent = prev; }, 2500);
      }
    });
    return;
  }
});

// Setup dynamic year, mobile nav, and recruiter contact form on load
document.addEventListener('DOMContentLoaded', () => {
  const yearEl = document.getElementById('year');
  if (yearEl) yearEl.textContent = new Date().getFullYear();

  const menu = document.querySelector('.menu-toggle');
  const nav = document.getElementById('site-nav');
  if (menu && nav) {
    menu.addEventListener('click', () => {
      const open = nav.classList.toggle('open');
      menu.setAttribute('aria-expanded', String(open));
    });

    nav.querySelectorAll('a').forEach(link => {
      link.addEventListener('click', () => {
        nav.classList.remove('open');
        menu.setAttribute('aria-expanded', 'false');
      });
    });
  }

  const recruiterForm = document.getElementById('recruiter-form');
  if (recruiterForm) {
    recruiterForm.addEventListener('submit', (e) => {
      e.preventDefault();
      const name = document.getElementById('recruiter-name')?.value.trim() || 'Hiring Team';
      const company = document.getElementById('recruiter-company')?.value.trim() || 'Organization';
      const role = document.getElementById('target-role')?.value || 'Data Engineer';
      const model = document.getElementById('work-model')?.value || 'Remote / Hybrid';
      const msg = document.getElementById('recruiter-msg')?.value.trim() || 'We would like to schedule an introductory conversation regarding our Data Engineering opening.';

      const subject = encodeURIComponent(`Interview Inquiry: ${role} (${model}) - ${company}`);
      const body = encodeURIComponent(
        `Hi Dylan,\n\n` +
        `${msg}\n\n` +
        `--- Inquiry Details ---\n` +
        `Recruiter / Hiring Manager: ${name}\n` +
        `Organization: ${company}\n` +
        `Target Role: ${role}\n` +
        `Work Model: ${model}\n`
      );

      window.location.href = `mailto:${profile.email}?subject=${subject}&body=${body}`;
    });
  }
});

