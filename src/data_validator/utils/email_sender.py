import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List

from ..config import EmailConfig, ValidationResult

logger = logging.getLogger(__name__)


class EmailSender:
    def __init__(self, config: EmailConfig):
        self.config = config
    
    def send_validation_report(self, results: List[ValidationResult]):
        """Send email report with validation results."""
        if not self.config.enabled:
            logger.info("Email notifications are disabled")
            return
        
        try:
            # Create email content
            subject = f"Data Validation Report - {len(results)} tables processed"
            body = self._create_report_body(results)
            
            # Send email
            self._send_email(subject, body)
            logger.info("Validation report email sent successfully")
            
        except Exception as e:
            logger.error(f"Failed to send email report: {e}")
    
    def _create_report_body(self, results: List[ValidationResult]) -> str:
        """Create HTML email body with validation results."""
        html = """
        <html>
        <head>
            <style>
                table { border-collapse: collapse; width: 100%; }
                th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                th { background-color: #4CAF50; color: white; }
                tr:nth-child(even) { background-color: #f2f2f2; }
                .success { color: green; }
                .warning { color: orange; }
                .error { color: red; }
            </style>
        </head>
        <body>
            <h2>Data Validation Report</h2>
            <table>
                <tr>
                    <th>Table Name</th>
                    <th>Status</th>
                    <th>Total Rows</th>
                    <th>Matched</th>
                    <th>Mismatched</th>
                    <th>Missing in Target</th>
                    <th>Extra in Target</th>
                    <th>Duration (sec)</th>
                </tr>
        """
        
        for result in results:
            status_class = 'success' if result.status == 'SUCCESS' else 'error' if result.status == 'FAILED' else 'warning'
            html += f"""
                <tr>
                    <td>{result.table_name}</td>
                    <td class="{status_class}">{result.status}</td>
                    <td>{result.total_rows:,}</td>
                    <td>{result.matched_rows:,}</td>
                    <td>{result.mismatched_rows:,}</td>
                    <td>{result.missing_in_target:,}</td>
                    <td>{result.extra_in_target:,}</td>
                    <td>{result.validation_duration_seconds:.2f}</td>
                </tr>
            """
        
        # Summary statistics
        total_tables = len(results)
        successful = len([r for r in results if r.status == 'SUCCESS'])
        failed = len([r for r in results if r.status == 'FAILED'])
        
        html += f"""
            </table>
            
            <h3>Summary</h3>
            <ul>
                <li>Total Tables Validated: {total_tables}</li>
                <li>Successful: {successful}</li>
                <li>Failed: {failed}</li>
            </ul>
        </body>
        </html>
        """
        
        return html
    
    def _send_email(self, subject: str, body: str):
        """Send email using SMTP."""
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.config.from_address
        msg['To'] = ', '.join(self.config.to_addresses)
        
        # Attach HTML content
        html_part = MIMEText(body, 'html')
        msg.attach(html_part)
        
        # Send email
        with smtplib.SMTP(self.config.smtp_host, self.config.smtp_port) as server:
            if self.config.use_tls:
                server.starttls()
            
            if self.config.smtp_username and self.config.smtp_password:
                server.login(self.config.smtp_username, self.config.smtp_password)
            
            server.send_message(msg)