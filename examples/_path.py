import sys, os, inspect
def add_root(up_levels=4):
    """
    Jupyter notebooks does not recognize packages based on where they are placed.
    This utility adds the parent path to sys.path, so they can import the package where they are placed in.
    """
    dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    for l in range(0, up_levels):
        dir = os.path.dirname(dir)
    sys.path.append(dir)
    #sys.path.insert(0, dir) 
    #print(f"added {dir}")