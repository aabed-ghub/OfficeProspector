from __future__ import annotations

import re
import smtplib
import socket
import time

import click
import dns.resolver
from tqdm import tqdm

from src.models.firm import Firm, Contact, EnrichmentSource


def _extract_domain(website: str) -> str:
    """Extract domain from a URL."""
    url = website.strip().lower()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    url = url.split("/")[0]
    return url


def _generate_patterns(first: str, last: str, domain: str, patterns: list[str]) -> list[str]:
    """Generate email guesses from name + domain + patterns."""
    first = first.lower().strip()
    last = last.lower().strip()
    if not first or not last or not domain:
        return []

    f = first[0]  # first initial

    emails = []
    for pattern in patterns:
        email = pattern.format(first=first, last=last, f=f)
        emails.append(f"{email}@{domain}")

    return emails


def _check_mx(domain: str) -> bool:
    """Check if domain has valid MX records."""
    try:
        answers = dns.resolver.resolve(domain, "MX")
        return len(answers) > 0
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer,
            dns.resolver.NoNameservers, dns.exception.Timeout):
        return False


def _verify_smtp(email: str, mx_host: str) -> bool | None:
    """Verify email via SMTP RCPT TO. Returns True/False/None (inconclusive)."""
    try:
        with smtplib.SMTP(mx_host, 25, timeout=10) as smtp:
            smtp.helo("verify.local")
            smtp.mail("test@verify.local")
            code, _ = smtp.rcpt(email)
            return code == 250
    except (smtplib.SMTPException, socket.error, OSError):
        return None


def _get_mx_host(domain: str) -> str | None:
    """Get the primary MX host for a domain."""
    try:
        answers = dns.resolver.resolve(domain, "MX")
        if answers:
            # Get highest priority (lowest preference number)
            mx = sorted(answers, key=lambda r: r.preference)[0]
            return str(mx.exchange).rstrip(".")
        return None
    except (dns.resolver.NXDOMAIN, dns.resolver.NoAnswer,
            dns.resolver.NoNameservers, dns.exception.Timeout):
        return None


def guess_and_verify_emails(firms: list[Firm], settings: dict) -> list[Firm]:
    """Generate email guesses for contacts and verify them."""
    enrich_settings = settings.get("enrich", {})
    patterns = enrich_settings.get("email_patterns", [
        "{first}.{last}", "{first}{last}", "{f}{last}", "{first}", "{last}",
    ])
    do_smtp = enrich_settings.get("email_verify_smtp", True)
    do_mx = enrich_settings.get("email_verify_mx", True)

    # Only process contacts that don't already have emails
    firms_needing_email = []
    for firm in firms:
        if not firm.website:
            continue
        contacts_without_email = [c for c in firm.contacts if not c.email and c.name]
        if contacts_without_email:
            firms_needing_email.append(firm)

    click.echo(f"  {len(firms_needing_email)} firms have contacts needing email guesses")

    # Cache MX results per domain
    mx_cache: dict[str, str | None] = {}
    mx_valid_cache: dict[str, bool] = {}
    guessed = 0
    verified = 0

    for firm in tqdm(firms_needing_email, desc="  Email guessing", unit=" firms"):
        domain = _extract_domain(firm.website)
        if not domain:
            continue

        # Check MX once per domain
        if domain not in mx_valid_cache:
            if do_mx:
                mx_valid_cache[domain] = _check_mx(domain)
                mx_cache[domain] = _get_mx_host(domain) if mx_valid_cache[domain] else None
            else:
                mx_valid_cache[domain] = True
                mx_cache[domain] = None

        if not mx_valid_cache[domain]:
            continue

        mx_host = mx_cache.get(domain)

        for contact in firm.contacts:
            if contact.email:
                continue
            if not contact.name:
                continue

            # Parse first/last from contact name
            name_parts = contact.name.split()
            if len(name_parts) < 2:
                continue
            first = name_parts[0]
            last = name_parts[-1]

            guesses = _generate_patterns(first, last, domain, patterns)
            if not guesses:
                continue

            # Try to verify each guess
            best_email = guesses[0]  # Default to most common pattern
            is_verified = False

            if do_smtp and mx_host:
                for email_guess in guesses:
                    result = _verify_smtp(email_guess, mx_host)
                    if result is True:
                        best_email = email_guess
                        is_verified = True
                        break
                    time.sleep(0.2)  # Be gentle with mail servers

            contact.email = best_email
            contact.email_verified = is_verified
            contact.source = EnrichmentSource.EMAIL_GUESS
            guessed += 1
            if is_verified:
                verified += 1

    click.echo(f"  Guessed {guessed} emails, verified {verified} via SMTP")
    return firms
