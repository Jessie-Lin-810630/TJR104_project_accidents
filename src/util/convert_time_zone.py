from timezonefinder import TimezoneFinder

tf = TimezoneFinder()


def get_timezone(lat: float, lng: float) -> str:
    """
    輸入經度與緯度，取得時區名稱。
    """
    return tf.timezone_at(lat=lat, lng=lng)


timezone_cache = {}


def get_timezone_cached(lat: float, lng: float) -> str:
    """
    將經緯度進位至小數點後二位後，判斷是否有對應的時區名稱已經存在dict快取層中，若有
    ，則不需要去爬或轉換時區；若無則需要爬取跟轉換時區。
    ps.有快取層的好處是對使用API爬時區的方案中能省些費用；對使用免費套件轉換時區的方案中能省些時間。

    """
    key = f"{round(lat, 2)},{round(lng, 2)}"

    if key in timezone_cache:
        return timezone_cache[key]

    tz = get_timezone(lat, lng)
    timezone_cache[key] = tz
    return tz


if __name__ == "__main__":
    print(get_timezone(23.8636197, 120.5864437))  # Asia/Taipei
    print(get_timezone_cached(23.8636197, 120.5864437))  # Asia/Taipei
