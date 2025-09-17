import { Router } from 'express';

const router = Router();

// Chat endpoint
router.post('/', async (req, res) => {
    const { message } = req.body;

    if (!message) {
        return res.status(400).json({ error: 'Message is required' });
    }

    // Placeholder response
    res.json({
        text: `You said: ${message}`,
        image: null,
    });
});

export default router;
