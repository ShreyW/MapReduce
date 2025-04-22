import os

with open("small_input.txt", "w") as f: 
    f.write(("one two three four five six seven" * 10000).strip() + "\n") 
    f.write("eight eight\n")