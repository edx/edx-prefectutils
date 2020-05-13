"""Console script for prefect_utils."""
import sys
import click


@click.command()
def main(args=None):
    """Console script for prefect_utils."""
    click.echo("Replace this message by putting your code into "
               "prefect_utils.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
