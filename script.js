
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
    disclosure: 'This case study describes my professional work. It leaves out the original source code, institutional data, credentials, and internal architecture.'[cite: 1]
  },
  fabric: {
    kicker: 'PROJECT 02 · DATA AUTOMATION',
    title: 'Automated Fabric Lakehouse Pipeline',
    intro: 'I used Microsoft Fabric, PySpark, and Delta tables to automate recurring financial and operational imports.',
    challenge: 'Roughly 30 daily month-to-date and year-to-date Excel extracts contained overlapping records and shifting formats, requiring manual reconciliation to ensure continuity with legacy imports.',
    contribution: 'I built PySpark notebooks in Fabric to ingest and standardize disparate workbooks, applied custom MTD and YTD business logic, and landed cleaned data from Bronze to Silver Delta tables. I then validated record counts and totals in Power BI against legacy imports.',
    outcome: 'Replaced manual daily file consolidation with an automated, idempotent pipeline, saving team hours and eliminating reporting discrepancies.',
    disclosure: 'This project comes from my previous professional work. Any code I publish later will use recreated logic and synthetic data.'[cite: 1]
  },
  ml: {
    kicker: 'PROJECT 03 · APPLIED MACHINE LEARNING',[cite: 1]
    title: 'Predictive Risk & Student Success Models',[cite: 1]
    intro: 'I worked on predictive models to help operational teams spot risks earlier.',[cite: 1]
    challenge: 'Support teams needed earlier warning that students might struggle or leave. Finance teams also needed a clearer view of repayment risk.',[cite: 1]
    contribution: 'I built the models in Pecan AI, tested which available features were useful, and refined the inputs. I focused on signals teams could use, including student outcomes early in a course and financial repayment risk using a 120-day bad-debt reference point.',[cite: 1]
    outcome: 'The predictions were intended to help teams intervene earlier and make better-informed operational decisions. Specific performance metrics are not shared publicly.',[cite: 1]
    disclosure: 'This case study explains the approach. It does not include student-level records, training data, proprietary model artifacts, or unverified performance claims.'[cite: 1]
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

const $= id => document.getElementById(id);[cite: 1]$('year').textContent = new Date().getFullYear();[cite: 1]

function addContactLink(id, href) {
  if (!href) return;[cite: 1]
  const node = $(id);[cite: 1]
  node.href = href;[cite: 1]
  node.hidden = false;[cite: 1]
}
addContactLink('email-link', profile.email ? `mailto:${profile.email}` : '');[cite: 1]
addContactLink('github-link', profile.github);[cite: 1]
addContactLink('linkedin-link', profile.linkedin);[cite: 1]
if (profile.email || profile.github || profile.linkedin) $('contact-hint').hidden = true;[cite: 1]

const dialog = $('project-dialog');[cite: 1]
document.querySelectorAll('[data-project]').forEach(button => {
  button.addEventListener('click', () => {
    const item = projectDetails[button.dataset.project];[cite: 1]
    if (!item) return;[cite: 1]
    ['kicker', 'title', 'intro', 'challenge', 'contribution', 'outcome', 'disclosure'].forEach(key => {
      $(`dialog-${key}`).textContent = item[key];[cite: 1]
    });
    dialog.showModal();[cite: 1]
  });
});
$('dialog-close').addEventListener('click', () => dialog.close());[cite: 1]
dialog.addEventListener('click', e => { if (e.target === dialog) dialog.close(); });[cite: 1]
const menu = document.querySelector('.menu-toggle');[cite: 1]
const nav = $('site-nav');[cite: 1]
menu.addEventListener('click', () => {
  const open = nav.classList.toggle('open');[cite: 1]
  menu.setAttribute('aria-expanded', String(open));[cite: 1]
  menu.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');[cite: 1]
});
nav.querySelectorAll('a').forEach(a => a.addEventListener('click', () => {
  nav.classList.remove('open');[cite: 1]
  menu.setAttribute('aria-expanded', 'false');[cite: 1]
  menu.setAttribute('aria-label', 'Open menu');[cite: 1]
}));[cite: 1]
