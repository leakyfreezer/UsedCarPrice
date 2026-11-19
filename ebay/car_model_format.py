

def formater(name, mark_orig):
    markAndModelSplit= name.split('_')
    if mark_orig.lower() == "mercedes_benz":
        if len(markAndModelSplit) > 0 and markAndModelSplit[0].lower() == "mercedes":
            if len(markAndModelSplit) == 0:
                return None
            markAndModelSplit.pop(0)
        if len(markAndModelSplit) > 0 and markAndModelSplit[1].lower() == "benz":
            if len(markAndModelSplit) == 0:
                return None
            markAndModelSplit.pop(0)
    if len(markAndModelSplit) > 0 and markAndModelSplit[0].lower() == mark_orig.lower():
        if len(markAndModelSplit) == 0:
            return None
        mark = markAndModelSplit.pop(0)
    
    if len(markAndModelSplit) == 0:
        return None
    model = markAndModelSplit.pop(0).lower()
    if model == mark_orig.lower():
        model = markAndModelSplit.pop(0).lower()

    return model
    