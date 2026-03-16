import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from zomato_cookie_store import save_cookies, load_cookies
import time

def create_zomato_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,application/xhtml+xml"
    })

    cookies = load_cookies()

    # 🔐 LOGIN ONLY ONCE
    if cookies is None:
        print("🔐 No valid cookies found — login required")
        cookies = get_zomato_session_cookies()
    else:
        print("✅ Reusing saved Zomato cookies")

    session.cookies.update(cookies)
    return session

def get_zomato_session_cookies():
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=opts)  # ✅ driver defined HERE
    driver.get("https://www.zomato.com/partners/onlineordering/reviews/")

    print("\n🔐 MANUAL LOGIN REQUIRED")
    print("1️⃣ Click LOGIN")
    print("2️⃣ Enter mobile number")
    print("3️⃣ Enter OTP")
    print("4️⃣ WAIT until dashboard loads\n")

    wait = WebDriverWait(driver, 300)  # wait up to 5 minutes

    try:
        # ✅ WAIT FOR REVIEWS TAB = LOGIN SUCCESS
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//a//div[normalize-space()='Reviews']")
            )
        )
        print("✅ Login successful — Reviews tab detected")

        # Optional: click Reviews tab
        reviews_tab = driver.find_element(
            By.XPATH, "//a//div[normalize-space()='Reviews']"
        )
        reviews_tab.click()
        time.sleep(2)

    except Exception as e:
        print("❌ Login failed or timeout")
        driver.quit()
        raise RuntimeError("Zomato login failed") from e

    # ---------------------------
    # Save cookies AFTER login
    # ---------------------------

    cookies = driver.get_cookies()
    driver.quit()  # ✅ close only AFTER success

    wanted = {
        "PHPSESSID",
        "_abck",
        "zat",
        "X-Zomato-Mx-Auth-Token",
        "zl",
        "fbcity"
    }

    final = {c["name"]: c["value"] for c in cookies if c["name"] in wanted}
    save_cookies(final)

    print("🍪 Cookies saved successfully")
    return final
