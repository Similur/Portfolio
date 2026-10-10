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
    disclosure: 'This case study describes my professional work. It leaves out the original source code, institutional data, credentials, and internal architecture.'
  },
  fabric: {
    kicker: 'PROJECT 02 · DATA AUTOMATION',
    title: 'Automated Fabric Lakehouse Pipeline',
    intro: 'I used Microsoft Fabric, PySpark, and Delta tables to automate recurring financial and operational imports.',
    challenge: 'Roughly 30 daily month-to-date and year-to-date Excel extracts contained overlapping records and shifting formats, requiring manual reconciliation to ensure continuity with legacy imports.',
    contribution: 'I built PySpark notebooks in Fabric to ingest and standardize disparate workbooks, applied custom MTD and YTD business logic, and landed cleaned data from Bronze to Silver Delta tables. I then validated record counts and totals in Power BI against legacy imports.',
    outcome: 'Replaced manual daily file consolidation with an automated, idempotent pipeline, saving team hours and eliminating reporting discrepancies.',
    disclosure: 'This project comes from my previous professional work. Any code I publish later will use recreated logic and synthetic data.'
  },
  ml: {
    kicker: 'PROJECT 03 · APPLIED MACHINE LEARNING',
    title: 'Predictive Risk & Student Success Models',
    intro: 'I worked on predictive models to help operational teams spot risks earlier.',
    challenge: 'Support teams needed earlier warning that students might struggle or leave. Finance teams also needed a clearer view of repayment risk.',
    contribution: 'I built the models in Pecan AI, tested which available features were useful, and refined the inputs. I focused on signals teams could use, including student outcomes early in a course and financial repayment risk using a 120-day bad-debt reference point.',
    outcome: 'The predictions were intended to help teams intervene earlier and make better-informed operational decisions. Specific performance metrics are not shared publicly.',
    disclosure: 'This case study explains the approach. It does not include student-level records, training data, proprietary model artifacts, or unverified performance claims.'
  },
  quality: {
    kicker: 'PROJECT 04 · DATA VALIDATION',
    title: 'Automated Data Quality & Reconciliation',
    intro: 'I designed automated validation patterns used across production data migrations and lakehouse pipelines.',
    challenge: 'A pipeline can run completely error-free yet still drop records, produce silent duplicates, or misalign financial totals downstream.',
    contribution: 'I built automated reconciliation suites using SQL and PySpark that cross-check source-to-target record counts, validate schemas, detect duplicate keys, and verify aggregated metrics prior to downstream consumption.',
    outcome: 'Gave analysts and stakeholders confidence in migration cutovers by catching data drift and edge-case drops before data reached production reports.',
    disclosure: 'The design reflects patterns from my production work and adheres to clean architecture principles without disclosing proprietary assets.'
  },
  crm: {
    kicker: 'PROJECT 05 · REST API & LAKEHOUSE',
    title: 'CRM-to-Silver Lakehouse Ingestion',
    intro: 'I built an automated PySpark pipeline in Microsoft Fabric to ingest CRM contacts directly into OneLake.',
    challenge: 'Upstream CRM APIs frequently evolve, and pulling raw contacts directly into operational models risks pipeline breakage from schema drift, duplicates, and unhandled pagination limits.',
    contribution: 'I wrote Fabric PySpark notebooks that stage raw JSON payloads in a Bronze Delta table with ingestion audit metadata. I then extracted, cleaned, and deduplicated records using windowing functions before performing idempotent merges into the Silver Delta layer.',
    outcome: 'Eliminated manual CRM data exports and provided analysts with clean, query-ready Delta tables optimized via Z-ordering for fast downstream Power BI reporting.',
    disclosure: 'This case study reflects architectural patterns from my professional work. All endpoints, credentials, and customer records have been omitted.'
  },
  docintel: {
    kicker: 'PROJECT 06 · AI & DOCUMENT INTELLIGENCE',
    title: 'Survey Sentiment & Executive Intelligence',
    intro: 'I built an automated processing pipeline using Azure Document Intelligence to surface actionable survey insights for leadership.',
    challenge: 'Valuable feedback arrived trapped in unstructured documents and survey forms, making it time-consuming for leadership to identify trends or take prompt action.',
    contribution: 'I built Python extraction workflows integrated with Azure Document Intelligence to parse unstructured text, run NLP sentiment scoring, and structure key themes into standardized reporting datasets.',
    outcome: 'Turned qualitative survey responses into quantifiable trends and executive-ready summaries, giving decision-makers immediate visibility into operational sentiment.',
    disclosure: 'This project describes my technical approach. Proprietary survey instruments, individual responses, and internal models are excluded.'
  }
};

document.addEventListener('DOMContentLoaded', ()
