/**
 * Google Apps Script — Shared Notes Backend for Office Prospector
 *
 * SETUP INSTRUCTIONS:
 * 1. Create a new Google Sheet
 * 2. Go to Extensions → Apps Script
 * 3. Paste this entire file into the script editor (replace any existing code)
 * 4. Click Deploy → New Deployment
 * 5. Choose "Web app" as the type
 * 6. Set "Execute as" → "Me"
 * 7. Set "Who has access" → "Anyone" (since the dashboard is on a private repo,
 *    only your team will have the Apps Script URL)
 * 8. Click Deploy and copy the Web App URL
 * 9. Paste the URL into the Office Prospector dashboard Settings panel
 *
 * The Google Sheet will automatically get a "Notes" sheet with columns:
 * EFIN | Status | Notes | Updated By | Updated At
 */

const SHEET_NAME = "Notes";
const HEADERS = ["EFIN", "Status", "Notes", "Updated At"];

function getOrCreateSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
    sheet.getRange(1, 1, 1, HEADERS.length).setValues([HEADERS]);
    sheet.getRange(1, 1, 1, HEADERS.length).setFontWeight("bold");
    sheet.setFrozenRows(1);
  }
  return sheet;
}

// Handle GET requests — return all notes as JSON
function doGet(e) {
  const sheet = getOrCreateSheet();
  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const notes = {};

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const efin = String(row[0]);
    if (efin) {
      notes[efin] = {
        status: row[1] || "",
        notes: row[2] || "",
        updatedAt: row[3] || "",
      };
    }
  }

  return ContentService.createTextOutput(JSON.stringify(notes))
    .setMimeType(ContentService.MimeType.JSON);
}

// Handle POST requests — upsert a note
function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents);
    const { efin, status, notes: noteText, timestamp } = payload;

    if (!efin) {
      return ContentService.createTextOutput(JSON.stringify({ error: "Missing EFIN" }))
        .setMimeType(ContentService.MimeType.JSON);
    }

    const sheet = getOrCreateSheet();
    const data = sheet.getDataRange().getValues();

    // Find existing row
    let rowIndex = -1;
    for (let i = 1; i < data.length; i++) {
      if (String(data[i][0]) === String(efin)) {
        rowIndex = i + 1; // 1-based
        break;
      }
    }

    const rowData = [efin, status || "", noteText || "", timestamp || new Date().toISOString()];

    if (rowIndex > 0) {
      // Update existing
      sheet.getRange(rowIndex, 1, 1, rowData.length).setValues([rowData]);
    } else {
      // Append new
      sheet.appendRow(rowData);
    }

    return ContentService.createTextOutput(JSON.stringify({ success: true }))
      .setMimeType(ContentService.MimeType.JSON);

  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ error: err.message }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
