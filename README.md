# Dylan Ferreira: Data Engineering Portfolio

A responsive portfolio with four data engineering and applied AI projects. It uses plain HTML, CSS, and JavaScript, so you can update and publish it without a build step.

## What's included

- A single page that adapts to different screen sizes, with a data pipeline illustration
- Four projects, each with a case study you can open from its card
- About, skills, and contact sections
- Accessible project dialogs and mobile navigation
- No API keys, customer data, external analytics scripts, or proprietary code are included

## Preview locally

1. Extract the ZIP file.
2. Open `index.html` in Chrome, Edge, or Firefox.
3. If you have Python installed, you can also run `python -m http.server 8000` from this folder and visit `http://localhost:8000`.

You can preview the site without running `npm install` or a build command.

## Customize before publishing

1. Add your email, GitHub URL, and LinkedIn URL to the `profile` object at the top of `script.js`. Leave a value empty to hide that button.
2. Check the biography, experience, technologies, and project summaries in `index.html`.
3. Check the full case studies in the `projectDetails` object in `script.js`.
4. To add a resume, put a PDF in `assets/` and link to it from `index.html` (optional).
5. Keep the text accurate when you edit it. Leave out confidential employer code, data, screenshots, connection details, and credentials.

## Publish with GitHub Pages

1. On GitHub, create a **public** repository named `portfolio` (or `yourusername.github.io` if you prefer a root-domain site). Do not initialize it with a README.
2. Open a terminal in the extracted project folder and run:

   ```bash
   git init
   git add .
   git commit -m "Add personal portfolio"
   git branch -M main
   git remote add origin https://github.com/YOUR_USERNAME/portfolio.git
   git push -u origin main
   ```

   Replace `YOUR_USERNAME` with your actual GitHub username, and use the matching repository name if you chose a different one. GitHub may prompt you to sign in.

3. In the GitHub repository, go to **Settings → Pages**.
4. Under **Build and deployment**, select **Deploy from a branch**. Choose branch **main** and folder **/(root)**, then save.
5. After publishing, your site will be at `https://YOUR_USERNAME.github.io/portfolio/`, or `https://YOUR_USERNAME.github.io/` if the repository was named `YOUR_USERNAME.github.io`.
6. Push updates with `git add . && git commit -m "Update portfolio" && git push`.

You can also upload the files through GitHub's **Add file → Upload files** menu. Upload the contents of this directory so `index.html` sits at the repository root, then configure Pages as described above.

### Notes

- The site uses Google Fonts and falls back to system fonts when offline.
- GitHub Pages can publish these files directly. You do not need React, Vite, or a custom domain.
- The project descriptions draw on professional experience. They leave out original employer code and data, and the data quality project is labeled as a representative example.


## Add your resume

1. Export your current resume as a PDF.
2. Rename the file to `resume.pdf`.
3. Place it next to `index.html` at the root of this project.
4. Upload the PDF to the same location in your GitHub Pages repository.

The **View resume** button on the homepage and **Download resume** button in the contact section both use `resume.pdf`. Add that file to make them work. The GitHub link already points to `https://github.com/Similur`.

Before publishing, check the PDF for personal contact details and hidden metadata. Anyone can access files in a public GitHub Pages repository.
