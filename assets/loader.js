document.addEventListener("DOMContentLoaded", function () {
    document.body.classList.add("dash-loading");

    const observer = new MutationObserver(() => {
        const loading = document.querySelector("._dash-loading");
        if (!loading) {
            document.body.classList.remove("dash-loading");
        }
    });

    observer.observe(document.body, { childList: true, subtree: true });
});
