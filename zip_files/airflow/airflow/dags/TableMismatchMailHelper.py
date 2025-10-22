"""
Module: TableMismatchMailHelper

Description:
    This module defines the `TableMismatchMailHelper` class, which helps out sending
    count mismatch between table status email.

Author:
    

Date:
    2025-02-18
"""
from airflow.operators.email import EmailOperator

class TableMismatchMailHelper:
    """
    TableMismatchMailHelper helps out sending mismatch table count status email.
    """
    def get_mismatched_tables(self, ti, **kwargs):
        """
        Prepares the mistmatch table list

        Parameters:
        ----------
        ti
            Task intance of the current airflow
        
        Returns:
        -------
        mismatches
            Returns the mismatched table lists
        """
        mismatches = ti.xcom_pull(task_ids="table_count_comparison")
        return mismatches

    def prepare_email_template(self, ti, **kwargs):
        """
        Prepares the email body

        Parameters:
        ----------
        ti
            Task intance of the current airflow
        
        Returns:
        -------
        email_template
            html email body content 
        """
        mismatched_tables = TableMismatchMailHelper.get_mismatched_tables(self, ti, **kwargs)
        email_template = ''
        
        if mismatched_tables:
            if not mismatched_tables[0] and not mismatched_tables[1]:
                email_template = f"""
                <!DOCTYPE html>
                <html>
                <head>
                </head>
                <body>
                <h2>No mismatches</h2>
                <p> There is no mismatch all tables have the same count </p>
                </body>
                </html>
                """  
            else:
                email_template = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <style>
                        table {{ border-collapse: collapse; width: 50%; margin-bottom: 20px; }}
                        th, td {{ border: 1px solid black; padding: 8px; text-align: left; }}
                    </style>
                </head>
                <body>
                    <h2>Mismatched table details</h2>
                    {''.join(f'''
                    <table>
                        <tr>
                            <th>table</th>
                            <th>source_count</th>
                            <th>destination_count</th>
                        </tr>
                        {''.join(f"<tr></td><td>{source_table}</td><td>{source_count}</td><td>{detination_count}</td></tr>"
                                for source_table, source_count, detination_count in sublist)}
                    </table>
                    ''' for sublist in mismatched_tables)}
                </body>
                </html>
                """
        return email_template

    def send_email(self, ti, **kwargs):
        """
        Prepares the EmailOperator and the send the email using email operator
        """
        email_template = TableMismatchMailHelper.prepare_email_template(self, ti, **kwargs)

        # Send the email using EmailOperator
        email_operator = EmailOperator(
            task_id="send_email",
            to="ananth.karnan@ideas2it.com",  # Replace with sender email address
            subject="Source and destination count check report",
            html_content=email_template,
            dag=kwargs['dag']
        )
        email_operator.execute(context=kwargs)

    def mail_trigger(self, ti, **kwargs):
        """
        Trigger the email operations
        """
        TableMismatchMailHelper.send_email(self, ti, **kwargs)
