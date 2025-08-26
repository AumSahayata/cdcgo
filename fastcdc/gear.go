package fastcdc

import "math/rand"

// The GearTable is a precomputed set of 256 random 64-bit values used for
// the rolling hash in content-defined chunking.
type GearTable [256]uint64

// Default gear table
var gearTable = GearTable{
	0xa8e274c800dc3426, 0x2283b1dba800c778, 0x7d18b312bdae38d4, 0xf23575802697a80f,
	0x021bd63f29d26cfb, 0x5483ea27943dc0fd, 0x3748335a62d263fe, 0x2f4652c43dc4df53,
	0x772d202dc11f17a0, 0x65b3d2487fe08ab8, 0x2030509db122a51b, 0x6390ba3011ff87e0,
	0x9a244c9dd0a30f18, 0xaef796c84341f1e2, 0x463810a0140bbe30, 0x06b1f6a59cf2e4b8,
	0xfb192cd4ddaf9639, 0xc4bc6c4097699222, 0x5615075404124e5d, 0xeee6dabc715e65c8,
	0x063f345a1a8d5e9f, 0x410f0751c77d457c, 0xe5872f972e503f1f, 0x9c63caac09dca796,
	0xa799423a28d01b2e, 0x016f76e7b9fa4e73, 0x7562064b04e31c61, 0xc411b06766a9c316,
	0x6d1275d8efc6bb07, 0x7fc269e351fcfaa4, 0xaf7acc1e9de5e879, 0x090052c66bcd8230,
	0x598db6abf4ade850, 0xb7786e01079447af, 0x3d8f9ced9faee7c9, 0x5eba681c0d6419b0,
	0xad2ac2408fcd840f, 0x5a673c290b6535b1, 0x4128a3294ea896fc, 0xd0db727721e66477,
	0x3bb13879af31209d, 0x1de069e44eed859e, 0xaf42548fd115c669, 0xeefdab8ecabcf37b,
	0x3af9c7da3f1619b2, 0x845f3194001c56d1, 0xb9eff31fccfbbbd0, 0x619013f8af84c430,
	0x90a60b69c5794afb, 0xe417c4146c9228f4, 0xcc6ab6f16a7675c3, 0x64cfe5f31c5713a4,
	0xe660a815bb350e7f, 0x538a278d0ed17d89, 0x4d3c88272ffae16c, 0xb890212748f05003,
	0x762fea9400580ded, 0x4baee480a336b022, 0xd334db0e7869334d, 0xde08ee4786fcfb36,
	0xc0d69dc8a5e7d60c, 0x48a440907b52dfa7, 0xd080c6ae4e9026e1, 0x37a3d9cea5293217,
	0x6dab36d9042eccfa, 0x56c118ca91287d33, 0x529973fb776f5e7d, 0x9cb288e518dd21bb,
	0xfb45eca569b093a4, 0x393ea352b5ce85ec, 0x39f94ecd82360633, 0x28421347b0d39b39,
	0xa74871d5d3163fe2, 0x7dc02421b4327997, 0xa9e4384b64d003f5, 0x33a8522663170af9,
	0x91fae1ac83c9aba9, 0x3d24b7458398b9ea, 0x6c91e083bc25ad7b, 0xf51ba6bea84ae52f,
	0x2429a4bbd9630be5, 0x7a9562ba670b4cb9, 0x0d52b8eac67a032e, 0xb661197302947e74,
	0x0d84d05aba8fa107, 0x739ce6d45d4b1041, 0x6c6febd54edb20da, 0x75f27d8502a83cf3,
	0x7ef136e4ae2daea1, 0xf0ad2eedf03a153a, 0x6fbc9a54827ed061, 0xc50b10705f360add,
	0x4da3698e155fd948, 0xd0fb8eb7803f1b89, 0xda1d2647d8a4267c, 0x099ef98c2aaea670,
	0x59acfb83582dc3ce, 0x3a7e195bca5d90d1, 0x14db57dbb0c963c3, 0x36bf562619996d18,
	0x22ca33c42973b8fb, 0xc9cc1ee345d4374f, 0x742eaab89667259d, 0x6a1490deb0b5ecda,
	0xccfa73ac4c9c52f2, 0x1f7813a144a70f17, 0xbf5a6f001ee33da4, 0x934c17cb23a98eb9,
	0xeb6b91934c6ee5f1, 0x4c2d69714888020d, 0xe65936677e0212cf, 0x21db475c47dd0135,
	0x992ffcc64bf41cfa, 0xb7ca2393d74dca32, 0x75f013aabe3bc31e, 0x6d1c3b57cdbbc1ed,
	0x859442cfaf1f3547, 0x3be3273992144288, 0x9396f75768ba2640, 0x9d9bcaa53b793b2a,
	0xdedb23dc91b413b8, 0xf6c200fee7960543, 0x8a046cba343cad4c, 0x611a7018e8ca9a00,
	0xae92ae17d8d62cf4, 0xcb9191391a5775d0, 0xa89c4dc20a3750fc, 0x2608e0651ca0ec7e,
	0x9046c783fb3762ca, 0x51f13ffb56bc6c6b, 0xcbab5ca3097d8c1c, 0x4ee15d2645e7da7c,
	0xe7a87ed7adedf61d, 0x43ee87b9fd823aec, 0xf7d089c09a88f850, 0xb4f5b0399a51fcc4,
	0x8d652af2d6d314f9, 0x461984cd10fcae30, 0x711d20f33a94c6d9, 0x88e505be1e14eb35,
	0x4fc35bacc25cc5ce, 0x9e31efa7db761edb, 0x03c6ba500b04b657, 0x2cb8f41293a4ad56,
	0xda1c1dade70a68b9, 0x314b3b3016e5b0bc, 0x01c14723ed391d5b, 0xf1227c2eaf475bf9,
	0x7c3cfb0e40ab65a1, 0xf07f65f512e10af9, 0x6badb96671d52230, 0x53a1d2a785b25cd9,
	0x413ab51cf0151c05, 0x81524ac7a9eb427a, 0x3f84ff0b32601064, 0x62dd8d866756b7da,
	0x3ab7c63d7139d5f2, 0x3e24ea466c5d71f7, 0xdb438de3ff1779cf, 0xbc7d3f37ab0e376b,
	0x0a19fad2c7a1cb39, 0xdfd35d33d207dd03, 0x50a4938f91664822, 0xc1c0a47e1a1c2dd2,
	0x85c7c514bd912e33, 0x57266f909a5691ab, 0xf4529c9f730ef41c, 0xec69a5d2fa5caecd,
	0xebeb026bd27793ea, 0x521a7f2a09a7168b, 0xd719acb8e2bfe2e5, 0x595daa7b030b24df,
	0x3c7ac898f8cca168, 0x0962c16535e6eb10, 0x2e68535b78bbef5c, 0x36e10c43b3f96b67,
	0xb6a73e3f4944e3bd, 0x30fb0a1d5b6e429f, 0x5c768a8362574cd3, 0x61f347c4d3892040,
	0xc35938d57dcfb47d, 0xbaf7f851807fa54c, 0x9a0d33799cf57a0f, 0x29a5b1a8b4f406ad,
	0x32e7b0e4025b798e, 0x6560b507decc7b29, 0x4fcc09955a2c6ace, 0x2c43d90c63b52f36,
	0x15321080112b61d0, 0xa1a20010c5e9b705, 0x13325781a8d18f4b, 0xc3f901161910366c,
	0x0016254d301e17a5, 0x12885de6c8451c5c, 0x4c080c9b6634d6dc, 0x374a6e454fa473fd,
	0x11759d42680e280c, 0xd29e91add5e2c92c, 0x2308d1cf8ad29153, 0x14bdb6038d0e64db,
	0xf309b00cb295dd94, 0x2c33838a6ffa8d3a, 0x1c5e0ba3069af768, 0xc795a950b0c5bf5a,
	0x840d361a91767b09, 0x334ac0b7167dd6e0, 0xdf971bd075cb01fd, 0xb4cb98d7d8b4e0db,
	0x1be90f883e8f75ea, 0x67a46c7158f8d494, 0x7abc2daad8f70fba, 0xabe2b26dbd4de11b,
	0x0ef6f83bf6a7c72a, 0x03e0b32cfefa2344, 0x23dd87b9d6edabd1, 0x77a4e7becd96a1fa,
	0x1d6cfb71b38c0b6d, 0xc73449cb7a2aedc8, 0x0fcc958b554db019, 0x532ae6c83ef3f949,
	0x15d3cf4016daa369, 0xc7b966777ebd2c44, 0x00b103028e51d837, 0xcf66ab654adebbe8,
	0xcb3b9736fc0ad126, 0xcd2184f47865bbcc, 0x7c8ea5d23241d100, 0x29df0a01061fba42,
	0x239996829147b928, 0xfa3c6899cdc2019c, 0xb2a8e4c6f920d9a3, 0xaa7292efc9b8d71b,
	0x1bfa947c9fabe298, 0xbba4db4e16c68813, 0x734c2be8dbe6b759, 0x120983386c342014,
	0xa04e7278b1facd56, 0xb8de48dfe80d6869, 0x533f0880bbabb6f5, 0xdf7325366caa0b1f,
	0xba30701dea69bf05, 0x217a9041639496b2, 0x92d734aac958a230, 0xa8da56038f2e8279,
	0x7e183c4512025814, 0xf2c1f2f5067202a8, 0x6caf86ecb5840f6a, 0x1c09c53b9d7aa616,
	0x6f79acb50864d775, 0xf02f3a72f11b760d, 0xa2c897d6a211ddff, 0x6a70d3ce5a19bcef,
	0x0c39e995c7e8d565, 0xf6c28ad4a1b5f635, 0x92206a0b0e669f67, 0xed7ffb4876c3f1b2,
}

// NewGearTableFromSeed generates a deterministic FastCDC GearTable from a given seed.
//
// Using the same seed produces the same table,
// ensuring deterministic chunk boundaries across multiple runs or machines.
//
// Parameters:
//   - seed: an int64 used to initialize the pseudo-random generator.
//
// Returns:
//   - GearTable: a newly generated GearTable with deterministic values.
func NewGearTableFromSeed(seed int64) GearTable {
	var gt GearTable

	r := rand.New(rand.NewSource(seed))
	for i := range gt {
		gt[i] = r.Uint64()
	}

	return gt
}

// GetGear returns the GearTable to use for a Chunker.
// If a custom table is provided in Params, it is returned;
// otherwise, the default gearTable is used.
func GetGear(p *Params) *GearTable {
	if p.Gear != nil {
		return p.Gear
	}
	return &gearTable
}
