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
    intro: 'I used Microsoft Fabric, PySpark, and Delta tables to automate repetitive imports.',
    challenge: 'The daily month-to-date and year-to-date extracts contained overlapping records. I needed to handle duplicates and compare the results accurately with earlier manual imports.',
    contribution: 'I wrote notebooks to process multiple Excel files and handle overlapping records as the data moved from Bronze to Silver Delta tables. Then I used Power BI to compare record counts and values with the existing manual imports.',
    outcome: 'The automated process reduced the need for manual daily imports. Reconciliation checks kept the results accurate.',
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
    intro: 'This example draws on checks used in production data migrations and pipelines.',
    challenge: 'A pipeline can finish without errors and still leave out records or create duplicates. Teams need reliable checks before they use the resulting tables.',
    contribution: 'The design compares record counts and source and target totals, checks schemas, and looks for duplicates. These are the kinds of checks I used with SQL and Power BI in previous projects.',
    outcome: 'This example provides a plan for checking data integrity. It represents techniques from my previous work; it is not a separate production deployment.',
    disclosure: 'The design is based on my professional experience. A public implementation and sample datasets could be developed separately.'
  }
};

const $ = id => document.getElementById(id);
$('year').textContent = new Date().getFullYear();

function addContactLink(id, href) {
  if (!href) return;
  const node = $(id);
  node.href = href;
  node.hidden = false;
}
addContactLink('email-link', profile.email ? `mailto:${profile.email}` : '');
addContactLink('github-link', profile.github);
addContactLink('linkedin-link', profile.linkedin);
if (profile.email || profile.github || profile.linkedin) $('contact-hint').hidden = true;

const dialog = $('project-dialog');
document.querySelectorAll('[data-project]').forEach(button => {
  button.addEventListener('click', () => {
    const item = projectDetails[button.dataset.project];
    if (!item) return;
    ['kicker', 'title', 'intro', 'challenge', 'contribution', 'outcome', 'disclosure'].forEach(key => {
      $(`dialog-${key}`).textContent = item[key];
    });
    dialog.showModal();
  });
});
$('dialog-close').addEventListener('click', () => dialog.close());
dialog.addEventListener('click', e => { if (e.target === dialog) dialog.close(); });
const menu = document.querySelector('.menu-toggle');
const nav = $('site-nav');
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
