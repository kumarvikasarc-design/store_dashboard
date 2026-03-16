from dash import html

def footer():
    return html.Div(
        [
            html.Img(
                src="/assets/logo.png",  # put logo in assets
                className="footer-logo",
            ),
            html.Span(
                "Powered by Coffee Island",
                className="footer-text",
            ),
        ],
        className="global-footer",
    )
