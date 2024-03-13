const puppeteer = require("puppeteer-extra");
const moment = require("moment");
const axios = require("axios");
const cron = require("node-cron");
// const { BigQuery } = require("@google-cloud/bigquery");

// const datasetId = "CakeLead";
// const tableId = "lead_table";

async function loadJsonToBigQuery() {
  const email = "vishaal.melwani@adquadrant.com";
  const password = "Adquadrant123!";
  const windowsLikePathRegExp = /[a-z]:\\/i;
  let inProduction = false;

  if (!windowsLikePathRegExp.test(__dirname)) {
    inProduction = true;
  }
  let options = {};
  if (inProduction) {
    options = {
      headless: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--media-cache-size=0",
        "--disk-cache-size=0",
        "--ignore-certificate-errors",
        "--ignore-certificate-errors-spki-list",
      ],
      timeout: 100000,
    };
  } else {
    options = {
      headless: false,
      timeout: 100000,
      args: [
        "--ignore-certificate-errors",
        "--ignore-certificate-errors-spki-list",
      ],
    };
  }
  const browser = await puppeteer.launch(options);
  const page = await browser.newPage();
  await page.setExtraHTTPHeaders({
    "User-Agent":
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
  });
  await page.setDefaultNavigationTimeout(100000);
  await page.goto("https://app.adquadrant.com/");
  await page.focus("input[name='u']").then(async () => {
    await page.keyboard.type(email, { delay: 100 });
  });
  await page.focus("input[name='p']").then(async () => {
    await page.keyboard.type(password, { delay: 100 });
  });
  await Promise.all([
    page.click("#submitButton"),
    page.waitForNavigation({ waitUntil: "load", timeout: 100000 }),
  ]).then(async (result) => {
    if (page.url() == "https://app.adquadrant.com/newaff.aspx") {
      const cookies = await page.cookies();
      let cookie = "";
      for (let idx in cookies) {
        cookie += cookies[idx].name + "=" + cookies[idx].value + "; ";
      }
      browser.close();
      let total = 0;
      let p = 0;
      let rows = [];
      do {
        const { data } = await axios.post(
          "https://app.adquadrant.com/Extjs.ashx?s=reportleadsbyaff",
          {
            groupBy: "",
            groupDir: "ASC",
            report_view_id: 124,
            report_id: 68,
            date_range: "custom",
            start_date: moment().subtract(1, "days").format("M/DD/YYYY"),
            end_date: moment().subtract(1, "days").format("M/DD/YYYY"),
            include_tests: 0,
            n: 200,
            o: "affiliate_name",
            d: "ASC",
            report_views: "lead_export",
            p: p,
          },
          {
            headers: {
              "Content-Type":
                "application/x-www-form-urlencoded; charset=UTF-8",
              Cookie: cookie,
              "X-Requested-With": "XMLHttpRequest",
            },
          }
        );
        total = data.total;
        p += 200;
        for (let row of data.rows) {
          rows.push({
            lead_id: row.unique_id,
            campaign_id: row.campaign_id,
            affiliate_name: row.affiliate_name,
            subid_1: row.subid_1,
            subid_2: row.subid_2,
            subid_3: row.subid_3,
            subid_4: row.subid_4,
            subid_5: row.subid_5,
          });
        }
      } while (p < total);

      console.log(rows);
      console.log(rows.length);
      const bigquery = new BigQuery();

      for (var i = 0; i < rows.length; i++) {
        await bigquery.dataset(datasetId).table(tableId).insert(rows[i]);
      }
    }
  });
}

// cron.schedule("*/10 0 0 * * *", function () {
//   console.log(
//     "========================Cron Job==============================="
//   );
  loadJsonToBigQuery();
});
