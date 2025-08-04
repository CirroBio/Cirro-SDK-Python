from pathlib import Path

import pdoc.render

if __name__ == "__main__":
    pdoc.render.configure(
        docformat="google",
        logo="https://static.cirro.bio/Cirro_Logo_Horizontal_Navy.png",
        logo_link="https://cirro.bio",
    )

    pdoc.pdoc(
        "cirro",
        "cirro.sdk",
        "cirro.auth",
        "cirro.services",
        "cirro.models",
        "cirro.config",
        "cirro.helpers.preprocess_dataset",
        "cirro_api_client",
        "cirro_api_client.cirro_client",
        "cirro_api_client.cirro_auth",
        "cirro_api_client.v1.api",
        "cirro_api_client.v1.client",
        "cirro_api_client.v1.models",
        output_directory=Path("./docs/")
    )

    # Redirect index.html to cirro.html since we want to expose that module first
    with Path('./docs/index.html').open('w') as index:
        index.write('<meta http-equiv="refresh" content="0; URL=cirro.html" />')
