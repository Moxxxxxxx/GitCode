
def strip(s):
    try:
        if s and isinstance(s, str):
            s = s.strip().replace(' ', '').replace('\n', '')
    except Exception as e:
        print(e)
    return s