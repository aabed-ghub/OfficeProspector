from __future__ import annotations

import re
import time
from urllib.parse import urljoin

import click
import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

from src.models.firm import Firm, Contact, EnrichmentSource

# Email regex pattern
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Common generic emails to skip
GENERIC_EMAILS = {"noreply@", "no-reply@", "donotreply@", "support@", "webmaster@",
                  "admin@", "postmaster@", "mailer-daemon@"}

# Staff-related keywords in page text
STAFF_KEYWORDS = {"owner", "partner", "principal", "founder", "director",
                  "president", "ceo", "managing", "cpa", "enrolled agent"}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def _normalize_url(url: str) -> str:
    """Ensure URL has a scheme."""
    url = url.strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _extract_emails(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Extract email addresses from page content and mailto links."""
    emails = set()

    # From mailto links
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.startswith("mailto:"):
            email = href.replace("mailto:", "").split("?")[0].strip().lower()
            if email and "@" in email:
                emails.add(email)

    # From page text
    text = soup.get_text()
    for match in EMAIL_RE.findall(text):
        email = match.lower()
        # Filter out image file extensions that look like emails
        if not email.endswith((".png", ".jpg", ".gif", ".svg", ".css", ".js")):
            emails.add(email)

    # Remove generic emails
    return [e for e in emails
            if not any(e.startswith(g) for g in GENERIC_EMAILS)]


def _extract_staff_names(soup: BeautifulSoup) -> list[dict]:
    """Try to extract staff names and titles from the page."""
    staff = []

    # Look for common patterns: headings followed by titles
    for heading in soup.find_all(["h2", "h3", "h4", "strong", "b"]):
        text = heading.get_text(strip=True)
        if not text or len(text) > 100:
            continue

        # Check if the next sibling contains a title
        next_el = heading.find_next_sibling()
        if next_el:
            next_text = next_el.get_text(strip=True).lower()
            if any(kw in next_text for kw in STAFF_KEYWORDS):
                staff.append({"name": text, "title": next_el.get_text(strip=True)})

    return staff


def _scrape_site(base_url: str, pages: list[str], timeout: int) -> dict:
    """Scrape a website and return extracted data."""
    result = {"emails": [], "staff": [], "phone": ""}
    all_emails = set()
    all_staff = []

    client = httpx.Client(headers=HEADERS, timeout=timeout, follow_redirects=True)

    try:
        for page_path in pages:
            url = urljoin(base_url, page_path)
            try:
                resp = client.get(url)
                if resp.status_code != 200:
                    continue

                soup = BeautifulSoup(resp.text, "lxml")

                # Extract emails
                emails = _extract_emails(soup, base_url)
                all_emails.update(emails)

                # Extract staff
                staff = _extract_staff_names(soup)
                all_staff.extend(staff)

                # Extract phone from tel: links
                if not result["phone"]:
                    for a_tag in soup.find_all("a", href=True):
                        href = a_tag["href"]
                        if href.startswith("tel:"):
                            phone = href.replace("tel:", "").strip()
                            phone = re.sub(r"[^\d\-\(\)\+\s]", "", phone)
                            if len(phone) >= 10:
                                result["phone"] = phone
                                break

            except (httpx.HTTPError, httpx.TimeoutException):
                continue
    finally:
        client.close()

    result["emails"] = list(all_emails)
    result["staff"] = all_staff
    return result


def enrich_with_websites(firms: list[Firm], settings: dict) -> list[Firm]:
    """Scrape firm websites for emails, staff info, and phone numbers."""
    enrich_settings = settings.get("enrich", {})
    pages = enrich_settings.get("scrape_pages", ["/", "/about", "/team", "/contact"])
    timeout = enrich_settings.get("scrape_timeout_seconds", 10)

    firms_with_sites = [f for f in firms if f.website]
    click.echo(f"  {len(firms_with_sites)} firms have websites to scrape")

    enriched = 0
    for firm in tqdm(firms_with_sites, desc="  Scraping websites", unit=" sites"):
        url = _normalize_url(firm.website)
        if not url:
            continue

        try:
            data = _scrape_site(url, pages, timeout)
        except Exception:
            continue

        # Add emails
        if data["emails"]:
            if not firm.email:
                firm.email = data["emails"][0]  # Best email as primary
            enriched += 1

        # Add phone
        if data["phone"] and not firm.phone:
            firm.phone = data["phone"]

        # Add staff as contacts
        for staff_member in data["staff"]:
            existing_names = {c.name.lower() for c in firm.contacts}
            name = staff_member["name"]
            if name.lower() not in existing_names:
                firm.contacts.append(Contact(
                    name=name,
                    title=staff_member.get("title", ""),
                    source=EnrichmentSource.WEBSITE,
                ))

        if EnrichmentSource.WEBSITE not in firm.enrichment_sources:
            firm.enrichment_sources.append(EnrichmentSource.WEBSITE)

        # Small delay to be polite
        time.sleep(0.5)

    click.echo(f"  Enriched {enriched} firms from website scraping")
    return firms
