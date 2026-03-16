document.addEventListener("click", function (e) {
    const link = e.target.closest("a");

    // Auto-hide when sidebar link clicked
    if (link && link.closest("#sidebar")) {
        document.getElementById("sidebar")?.classList.add("hide");
        document.getElementById("page-content")?.classList.add("collapsed");
    }

    // Toggle button
    if (e.target.id === "sidebar-toggle") {
        document.getElementById("sidebar")?.classList.toggle("hide");
        document.getElementById("page-content")?.classList.toggle("collapsed");
    }
});
