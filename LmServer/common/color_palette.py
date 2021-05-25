"""Module containing a class for generating color palettes

Note:
    Written by Dave Vieglais
"""
import math


class ColorPalette:
    """Implements a simple color palette generator.

    Note:
        * Intended for use in providing a color scale for rendering continuous
            values.
        * May be treated like a list
    """

    def __init__(self, ptype='pretty', n=255, alpha=0):
        """Constructor for ColorPalette

        Args:
            ptype: name of the palette to use:
                pretty, gray, red, green, blue, or safe.
            n: number of bins/gradients to use for the color range
            alpha: opacity for color, 0 is transparent, 255 is opaque.
        """
        self.pal = []
        if n > 0:
            self.n = n
        else:
            raise Exception('Number of bins (n) must be greater than 0')
        self.ptype = ptype
        self.alpha = alpha
        self.ptypes = {"gray": self.grayscale,
                       "red": self.redscale,
                       "green": self.greenscale,
                       "blue": self.bluescale,
                       "yellow": self.yellowscale,
                       "fuschia": self.fuschiascale,
                       "aqua": self.aquascale,
                       "safe": self.safetycolors,
                       "pretty": self.prettyscale,
                       "bluered": self.blueredscale,
                       "bluegreen": self.bluegreenscale,
                       "greenred": self.greenredscale}
        if self.ptype not in list(self.ptypes.keys()):
            self.ptype = "gray"
        self.method = self.ptypes[self.ptype]
        self.method()

    def keys(self):
        """Get pallete keys."""
        return list(self.ptypes.keys())

    def __getitem__(self, key):
        return self.pal.__getitem__(key)

    def __len__(self):
        return self.pal.__len__()

    def __iter__(self):
        return self.pal.__iter__()

    def __contains__(self, item):
        return self.pal.__contains__(item)

    def __delitem__(self, key):
        return self.pal.__delitem__(key)

    def __setitem__(self, key, value):
        return self.pal.__setitem__(key, value)

    def grayscale(self):
        """Standard grayscale."""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            v = int(i * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([v, v, v, a])

    def redscale(self):
        """Reds from black to very red."""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            v = int(i * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([v, 0, 0, a])

    def greenscale(self):
        """Greens from black to green"""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            v = int(i * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([0, v, 0, a])

    def bluescale(self):
        """Blues from black to blue."""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            v = int(i * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([0, 0, v, a])

    def yellowscale(self):
        """Yellows from black to yellow."""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            v = int(i * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([v, v, 0, a])

    def fuschiascale(self):
        """Fuschias from black to fuschia."""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            v = int(i * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([v, 0, v, a])

    def aquascale(self):
        """Aquas from black to aqua."""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            v = int(i * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([0, v, v, a])

    def safetycolors(self):
        """
        Generates a palette of 125 "web safe" colors.
        Note that this palette does not provide a gradient suitable
        for rendering continuous values.
        """
        self.pal = []
        a = 0
        idx = 0
        for i in range(0, 6):
            b = i * 0x33
            for j in range(0, 6):
                g = j * 0x33
                for k in range(0, 6):
                    r = k * 0x33
                    if idx == self.alpha:
                        a = 0
                    else:
                        a = 255
                    self.pal.append([r, g, b, a])
                    idx = idx + 1

    def prettyscale(self):
        """
        Makes a palette of appealing colors (to me anyway) for a continuous
        gradient.  The colors and intensities are intended to maximize
        perceived separation of values across the range.
        """
        self.pal = []
        a = 0
        rscl = [0.0, -1.0, math.pi / (self.n * 0.8)]
        gscl = [0.3, 0.7, math.pi / (self.n * 1.0)]
        bscl = [0.0, 1.0, math.pi / (self.n * 1.5)]
        for i in range(0, self.n + 1):
            r = rscl[0] + rscl[1] * math.cos(i * rscl[2])
            g = gscl[0] + gscl[1] * math.sin(i * gscl[2])
            b = bscl[0] + bscl[1] * math.cos(i * bscl[2])
            if r < 0:
                r = 0
            elif r > 1.0:
                r = 1.0
            if g < 0:
                g = 0
            elif g > 1.0:
                g = 1.0
            if b < 0:
                b = 0
            elif b > 1.0:
                b = 1.0
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([int(r * 255), int(g * 255), int(b * 255), a])

    def blueredscale(self):
        """Colors ranging from blue through to red"""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            vr = int(i * scl)
            vb = int((self.n - i) * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([vr, 0, vb, a])

    def bluegreenscale(self):
        """Colors ranging from blue through to green"""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            vg = int(i * scl)
            vb = int((self.n - i) * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([0, vg, vb, a])

    def greenredscale(self):
        """Colors ranging from green through to red"""
        self.pal = []
        a = 0
        scl = 255.0 / (self.n * 1.0)
        for i in range(0, self.n + 1):
            vr = int(i * scl)
            vg = int((self.n - i) * scl)
            if i == self.alpha:
                a = 0
            else:
                a = 255
            self.pal.append([vr, vg, 0, a])
