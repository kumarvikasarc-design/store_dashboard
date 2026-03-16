let lastScrollY = window.scrollY;

window.addEventListener("scroll", () => {
    const footer = document.querySelector(".global-footer");
    if (!footer) return;

    if (window.scrollY > lastScrollY) {
        // scrolling down → hide
        footer.classList.add("hide");
    } else {
        // scrolling up → show
        footer.classList.remove("hide");
    }

    lastScrollY = window.scrollY;
});
